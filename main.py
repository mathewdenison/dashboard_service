import asyncio
import json
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.templating import Jinja2Templates
import pkg_resources
from google.cloud import pubsub_v1

# ----------------------------
# Logging Setup
# ----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard_service")

# ----------------------------
# FastAPI Setup
# ----------------------------
app = FastAPI()

class ConnectionManager:
    def __init__(self):
        # active_connections is a dict mapping employee_id (as str) to a list of WebSocket connections.
        self.active_connections = {}
        # latest_messages stores, per employee, the latest message per type.
        self.latest_messages = {}

    async def connect(self, websocket: WebSocket, employee_id: str):
        await websocket.accept()
        employee_id = str(employee_id)
        if employee_id not in self.active_connections:
            self.active_connections[employee_id] = []
        self.active_connections[employee_id].append(websocket)
        logger.info(f"Employee {employee_id} connected. Total connections: {len(self.active_connections[employee_id])}")
        # Send any latest messages for this employee.
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
        if employee_id.lower() == "all":
            for emp_id in self.active_connections.keys():
                if emp_id not in self.latest_messages:
                    self.latest_messages[emp_id] = {}
                try:
                    parsed = json.loads(message)
                    msg_type = parsed.get("type", "data")
                except Exception:
                    msg_type = "data"
                self.latest_messages[emp_id][msg_type] = message
                for connection in self.active_connections[emp_id]:
                    try:
                        await connection.send_text(message)
                    except Exception as e:
                        logger.exception("Error sending bulk message to employee %s: %s", emp_id, e)
        else:
            employee_id = str(employee_id)
            try:
                parsed = json.loads(message)
                msg_type = parsed.get("type", "data")
            except Exception:
                msg_type = "data"
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

async def send_keepalive(websocket: WebSocket, interval: int = 30):
    try:
        while True:
            await asyncio.sleep(interval)
            await websocket.send_text('{"type": "ping"}')
    except Exception as e:
        logger.exception("Keepalive error: %s", e)

@app.websocket("/ws/dashboard")
async def websocket_endpoint(
        websocket: WebSocket,
        employee_id: str = Query(...),
):
    employee_id = str(employee_id)
    await manager.connect(websocket, employee_id)
    keepalive_task = asyncio.create_task(send_keepalive(websocket))
    try:
        while True:
            data = await websocket.receive_text()
            logger.info(f"Received from employee {employee_id}: {data}")
    except WebSocketDisconnect:
        keepalive_task.cancel()
        manager.disconnect(websocket, employee_id)

# ----------------------------
# Google Pub/Sub Setup
# ----------------------------
project_id = "hopkinstimesheetproj"
subscription_name = "dashboard-sub"
subscription_path = f"projects/{project_id}/subscriptions/{subscription_name}"
subscriber = pubsub_v1.SubscriberClient()

# We no longer use Redis. Instead, the Pub/Sub callback will directly broadcast messages.
def pubsub_callback(message: pubsub_v1.subscriber.message.Message):
    try:
        data_str = message.data.decode("utf-8")
        logger.info("Raw Pub/Sub message data: %s", data_str)
        payload = json.loads(data_str) if data_str else None
        if not payload or not isinstance(payload, dict):
            logger.error("Received empty or invalid payload: %s", data_str)
            message.ack()  # Acknowledge invalid messages to avoid retry
            return

        employee_id = str(payload.get("employee_id"))
        if employee_id.lower() == "all":
            logger.info("Received bulk message for all employees: %s", payload)
        else:
            logger.info("Received Pub/Sub message for employee %s: %s", employee_id, payload)

        # Use the global event loop to run the broadcast coroutine.
        future = asyncio.run_coroutine_threadsafe(
            manager.broadcast(json.dumps(payload), employee_id), event_loop
        )
        future.result()
        message.ack()
    except Exception as e:
        logger.exception("Error processing Pub/Sub message. Raw data: %s. Exception: %s",
                         message.data.decode("utf-8"), e)
        message.nack()

def start_pubsub_listener():
    future = subscriber.subscribe(subscription_path, callback=pubsub_callback)
    logger.info("Started Pub/Sub listener for dashboard service")
    try:
        future.result()  # Blocks indefinitely.
    except Exception as e:
        logger.exception("Error in Pub/Sub listener: %s", e)
        future.cancel()

# ----------------------------
# Startup Event
# ----------------------------
event_loop = asyncio.get_event_loop()

@app.on_event("startup")
async def startup_event():
    global event_loop
    event_loop = asyncio.get_event_loop()
    # Remove Redis initialization; no Redis is used in this updated version.
    # Instead, we only start the Pub/Sub listener in an executor.
    event_loop.run_in_executor(None, start_pubsub_listener)
