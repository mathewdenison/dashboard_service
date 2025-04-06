import asyncio
import json
import logging
import os
from fastapi import FastAPI
import socketio
from google.cloud import pubsub_v1

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard_service")

# --- Socket.IO Server Setup ---
# Create an Async Socket.IO server; here we use asyncio as async_mode.
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
app = FastAPI()
# Mount the Socket.IO ASGI app on a specific path (default is /socket.io)
sio_app = socketio.ASGIApp(sio, other_asgi_app=app)

# --- Socket.IO Event Handlers ---
@sio.event
async def connect(sid, environ, auth):
    """
    Connection event handler.
    Expects 'employee_id' (and optionally an auth token) to be provided in the auth dict.
    """
    employee_id = auth.get("employee_id")
    logger.info(f"Client connected: {sid}, employee: {employee_id}")
    if not employee_id:
        # Reject connection if no employee_id provided.
        await sio.disconnect(sid)
    else:
        # Save session and join a room identified by employee_id.
        await sio.save_session(sid, {"employee_id": employee_id})
        await sio.enter_room(sid, employee_id)

@sio.event
async def disconnect(sid):
    session = await sio.get_session(sid)
    employee_id = session.get("employee_id") if session else None
    logger.info(f"Client disconnected: {sid}, employee: {employee_id}")

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
        # Schedule the Socket.IO emit on the event loop.
        future = asyncio.run_coroutine_threadsafe(
            sio.emit("dashboard_update", payload, room=employee_id), event_loop
        )
        # Optionally wait for the emit to complete:
        future.result()
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

# --- Expose the Socket.IO app ---
# In your deployment, make sure to run the ASGI app (sio_app) instead of the FastAPI app directly.
# For example, with uvicorn:
#   uvicorn main:sio_app --host 0.0.0.0 --port 8000
