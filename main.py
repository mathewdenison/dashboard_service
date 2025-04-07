import asyncio
import json
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from google.cloud import pubsub_v1
import aioredis  # For async Redis access

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard_service")

# --- FastAPI Setup ---
app = FastAPI()

# Updated ConnectionManager that stores only the latest message of each type per employee.
class ConnectionManager:
    def __init__(self):
        # Dictionary mapping employee_id (as a string) to list of WebSocket connections.
        self.active_connections = {}
        # Dictionary to store the latest message per type for each employee.
        # Format: { employee_id: { msg_type: message_string, ... }, ... }
        self.latest_messages = {}

    async def connect(self, websocket: WebSocket, employee_id: str):
        await websocket.accept()
        employee_id = str(employee_id)
        if employee_id not in self.active_connections:
            self.active_connections[employee_id] = []
        self.active_connections[employee_id].append(websocket)
        logger.info(f"Employee {employee_id} connected. Total connections: {len(self.active_connections[employee_id])}")

        # When a new connection is established, send the latest messages (if any) to the client.
        if employee_id in self.latest_messages:
            for msg in self.latest_messages[employee_id].values():
                try:
                    await websocket.send_text(msg)
                except Exception as e:
                    logger.exception("Error sending latest message to employee %s: %s", employee_id, e)

    def disconnect(self, websocket: WebSocket, employee_id: str):
        employee_id = str(employee_id)
        if employee_id in self.active_connections:
            self.active_connections[employee_id].remove(websocket)
            logger.info(f"Employee {employee_id} disconnected. Remaining: {len(self.active_connections[employee_id])}")

    async def broadcast(self, message: str, employee_id: str):
        employee_id = str(employee_id)
        # Attempt to parse the message to extract its type.
        try:
            parsed = json.loads(message)
            msg_type = parsed.get("type")
        except Exception as e:
            logger.exception("Error parsing message for employee %s: %s", employee_id, e)
            msg_type = None

        # If message type exists, store/update it in the latest_messages.
        if msg_type:
            if employee_id not in self.latest_messages:
                self.latest_messages[employee_id] = {}
            self.latest_messages[employee_id][msg_type] = message

        if employee_id in self.active_connections:
            for connection in self.active_connections[employee_id]:
                try:
                    await connection.send_text(message)
                except Exception as e:
                    logger.exception("Error sending message to employee %s: %s", employee_id, e)

manager = ConnectionManager()

# Keep-alive task: send periodic pings to keep the connection alive.
async def send_keepalive(websocket: WebSocket, interval: int = 30):
    try:
        while True:
            await asyncio.sleep(interval)
            await websocket.send_text('{"type": "ping"}')
    except Exception as e:
        logger.exception("Keepalive error: %s", e)

# --- WebSocket Endpoint ---
@app.websocket("/ws/dashboard")
async def websocket_endpoint(
        websocket: WebSocket,
        employee_id: str = Query(...),
        auth_token: str = Query(...)
):
    # TODO: Insert token verification logic if needed.
    employee_id = str(employee_id)
    await manager.connect(websocket, employee_id)
    keepalive_task = asyncio.create_task(send_keepalive(websocket))
    try:
        while True:
            data = await websocket.receive_text()
            logger.info(f"Received from employee {employee_id}: {data}")
            # Optionally process incoming messages from the client.
    except WebSocketDisconnect:
        keepalive_task.cancel()
        manager.disconnect(websocket, employee_id)

# --- Google Pub/Sub Setup ---
project_id = "hopkinstimesheetproj"
subscription_name = "dashboard-sub"
subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"
subscriber = pubsub_v1.SubscriberClient()

# --- Redis Setup ---
# Since Redis is installed without authentication, we connect without a password.
REDIS_URL = "redis://redis-master.default.svc.cluster.local:6379"
redis = None  # Will be initialized on startup

async def init_redis():
    global redis
    redis = await aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    logger.info("Redis connection established")

async def redis_listener():
    """Subscribe to the Redis channel and broadcast messages to local WebSocket connections."""
    pubsub_redis = redis.pubsub()
    await pubsub_redis.subscribe("dashboard_channel")
    logger.info("Subscribed to Redis channel: dashboard_channel")
    while True:
        msg = await pubsub_redis.get_message(ignore_subscribe_messages=True, timeout=1.0)
        if msg:
            try:
                data = msg["data"]
                parsed = json.loads(data)
                employee_id = str(parsed.get("employee_id"))
                msg_type = parsed.get("type", "data")
                logger.info(f"Redis received message for employee {employee_id} with type '{msg_type}': {parsed}")
                await manager.broadcast(data, employee_id)
            except Exception as e:
                logger.exception("Error processing Redis message: %s", e)
        await asyncio.sleep(0.1)

# --- Pub/Sub Callback (Publishing to Redis) ---
def pubsub_callback(message: pubsub_v1.subscriber.message.Message):
    try:
        data_str = message.data.decode("utf-8")
        logger.info("Raw Pub/Sub message data: %s", data_str)
        payload = json.loads(data_str) if data_str else None

        if not payload or not isinstance(payload, dict):
            logger.error("Received empty or invalid payload: %s", data_str)
            message.ack()  # Or message.nack() if you want to retry
            return

        employee_id = str(payload.get("employee_id"))
        if employee_id.lower() == "all":
            logger.info("Received bulk PTO message for all employees: %s", payload)
        else:
            logger.info("Received Pub/Sub message for employee %s: %s", employee_id, payload)

        future = asyncio.run_coroutine_threadsafe(
            redis.publish("dashboard_channel", json.dumps(payload)), event_loop
        )
        future.result()  # Optionally wait for the publish to complete.
        message.ack()
    except Exception as e:
        logger.exception("Error processing Pub/Sub message. Raw data: %s. Exception: %s", message.data.decode("utf-8"), e)
        message.nack()



def start_pubsub_listener():
    future = subscriber.subscribe(subscription_path, callback=pubsub_callback)
    logger.info("Started Pub/Sub listener for dashboard service")
    try:
        future.result()  # This will block indefinitely unless an error occurs.
    except Exception as e:
        logger.exception("Error in Pub/Sub listener: %s", e)

# --- Startup Event ---
event_loop = asyncio.get_event_loop()

@app.on_event("startup")
async def startup_event():
    global event_loop
    event_loop = asyncio.get_event_loop()
    await init_redis()
    event_loop.create_task(redis_listener())
    event_loop.run_in_executor(None, start_pubsub_listener)
