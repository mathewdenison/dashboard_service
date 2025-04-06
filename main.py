import asyncio
import json
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from google.cloud import pubsub_v1

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard_service")

# --- FastAPI and WebSocket Setup ---
app = FastAPI()

# A connection manager to track active WebSocket connections by employee_id.
class ConnectionManager:
    def __init__(self):
        # Dictionary mapping employee_id to list of WebSocket connections
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

# --- WebSocket Endpoint ---
@app.websocket("/ws/dashboard")
async def websocket_endpoint(
        websocket: WebSocket,
        employee_id: str = Query(...),
        auth_token: str = Query(...)
):
    # TODO: Insert token verification logic here if needed.
    await manager.connect(websocket, employee_id)
    try:
        while True:
            data = await websocket.receive_text()
            logger.info(f"Received from employee {employee_id}: {data}")
            # Optionally process incoming messages from the client.
    except WebSocketDisconnect:
        manager.disconnect(websocket, employee_id)

# --- Google Pub/Sub Setup ---
project_id = "hopkinstimesheetproj"
subscription_name = "dashboard-sub"
subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"
subscriber = pubsub_v1.SubscriberClient()

# We'll store a reference to the event loop so that we can schedule coroutines from Pub/Sub callbacks.
event_loop = asyncio.get_event_loop()

def pubsub_callback(message: pubsub_v1.subscriber.message.Message):
    try:
        payload = json.loads(message.data.decode("utf-8"))
        employee_id = payload.get("employee_id")
        logger.info(f"Received Pub/Sub message for employee {employee_id}: {payload}")
        # Schedule the broadcast on the event loop.
        future = asyncio.run_coroutine_threadsafe(
            manager.broadcast(json.dumps(payload), employee_id), event_loop
        )
        future.result()  # Optionally wait for the broadcast to complete.
        message.ack()
    except Exception as e:
        logger.exception("Error processing Pub/Sub message")
        message.nack()

def start_pubsub_listener():
    future = subscriber.subscribe(subscription_path, callback=pubsub_callback)
    logger.info("Started Pub/Sub listener for dashboard service")
    try:
        future.result()  # This will block indefinitely unless an error occurs.
    except Exception as e:
        logger.exception("Error in Pub/Sub listener: %s", e)

@app.on_event("startup")
async def startup_event():
    global event_loop
    event_loop = asyncio.get_event_loop()
    # Run the blocking Pub/Sub listener in a separate thread so it doesn't block the event loop.
    event_loop.run_in_executor(None, start_pubsub_listener)
