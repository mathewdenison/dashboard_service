import asyncio
import json
import logging
import os
from typing import Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from google.cloud import pubsub_v1

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard_service")

# FastAPI app
app = FastAPI()

# A simple connection manager to track WebSocket connections by employee_id.
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, employee_id: str):
        await websocket.accept()
        self.active_connections.setdefault(employee_id, []).append(websocket)
        logger.info(f"Employee {employee_id} connected. Total connections: {len(self.active_connections[employee_id])}")

    def disconnect(self, websocket: WebSocket, employee_id: str):
        if employee_id in self.active_connections:
            self.active_connections[employee_id].remove(websocket)
            if not self.active_connections[employee_id]:
                del self.active_connections[employee_id]
            logger.info(f"Employee {employee_id} disconnected.")

    async def broadcast(self, message: str, employee_id: str):
        if employee_id in self.active_connections:
            for connection in self.active_connections[employee_id]:
                await connection.send_text(message)
            logger.info(f"Broadcasted message to employee {employee_id}")

manager = ConnectionManager()

# WebSocket endpoint
@app.websocket("/ws/dashboard")
async def websocket_endpoint(
        websocket: WebSocket,
        employee_id: str = Query(...),
        auth_token: str = Query(...)
):
    # TODO: Add your token verification logic here.
    # For now we simply accept the connection.
    await manager.connect(websocket, employee_id)
    try:
        while True:
            # Wait for any incoming message (if you need to receive messages from client)
            data = await websocket.receive_text()
            logger.info(f"Received from employee {employee_id}: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket, employee_id)

# --- Pub/Sub Setup ---

project_id = "hopkinstimesheetproj"
subscription_name = "dashboard-sub"
subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"

subscriber = pubsub_v1.SubscriberClient()

def pubsub_callback(message: pubsub_v1.subscriber.message.Message):
    try:
        payload = json.loads(message.data.decode("utf-8"))
        employee_id = payload.get("employee_id")
        logger.info(f"Received Pub/Sub message for employee {employee_id}: {payload}")
        # Schedule the broadcast on the event loop
        asyncio.run(manager.broadcast(json.dumps(payload), employee_id))
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

# Start the Pub/Sub listener in a background thread when the app starts.
@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_event_loop()
    # Run the blocking Pub/Sub listener in an executor so it doesn't block the event loop.
    loop.run_in_executor(None, start_pubsub_listener)
