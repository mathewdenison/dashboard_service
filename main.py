import asyncio
import json
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from google.cloud import pubsub_v1
import aioredis  # For async Redis access

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard_service")

# --- FastAPI and WebSocket Setup ---
app = FastAPI()

# A connection manager to track active WebSocket connections by employee_id.
class ConnectionManager:
    def __init__(self):
        # Dictionary mapping employee_id to list of WebSocket connections.
        self.active_connections = {}

    async def connect(self, websocket: WebSocket, employee_id: str):
        await websocket.accept()
        if employee_id not in self.active_connections:
            self.active_connections[employee_id] = []
        self.active_connections[employee_id].append(websocket)
        logger.info(f"Employee {employee_id} connected. Total connections: {len(self.active_connections[employee_id])}")

    def disconnect(self, websocket: WebSocket, employee_id: str):
        if employee_id in self.active_connections:
            self.active_connections[employee_id].remove(websocket)
            logger.info(f"Employee {employee_id} disconnected. Remaining: {len(self.active_connections[employee_id])}")

    async def broadcast(self, message: str, employee_id: str):
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
# Since you reinstalled Redis without authentication, we connect without a password.
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
                employee_id = parsed.get("employee_id")
                logger.info(f"Redis received message for employee {employee_id}: {parsed}")
                await manager.broadcast(data, employee_id)
            except Exception as e:
                logger.exception("Error processing Redis message: %s", e)
        await asyncio.sleep(0.1)

# --- Pub/Sub Callback (Publishing to Redis) ---
def pubsub_callback(message: pubsub_v1.subscriber.message.Message):
    try:
        payload = json.loads(message.data.decode("utf-8"))
        employee_id = payload.get("employee_id")
        logger.info(f"Received Pub/Sub message for employee {employee_id}: {payload}")
        # Publish the payload to the Redis channel
        future = asyncio.run_coroutine_threadsafe(
            redis.publish("dashboard_channel", json.dumps(payload)), event_loop
        )
        future.result()  # Optionally wait for the publish to complete.
        message.ack()
    except Exception as e:
        logger.exception("Error processing Pub/Sub message: %s", e)
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
    # Initialize Redis connection.
    await init_redis()
    # Start the Redis listener as a background task.
    event_loop.create_task(redis_listener())
    # Run the blocking Pub/Sub listener in an executor.
    event_loop.run_in_executor(None, start_pubsub_listener)
