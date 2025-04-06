import eventlet
eventlet.monkey_patch()

import json
import os
import logging
import threading
import requests

from flask import Flask, request
from flask_socketio import SocketIO, join_room, leave_room, disconnect
from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging

# --- Google Cloud Logging Setup ---
cloud_log_client = cloud_logging.Client()
cloud_log_client.setup_logging()

logger = logging.getLogger("dashboard_service")
logger.setLevel(logging.INFO)

# --- Flask and SocketIO Setup ---
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", path="/api/dashboard/socket.io", async_mode="eventlet")

# --- Google Pub/Sub Setup ---
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

project_id = "hopkinstimesheetproj"
subscription_path = f"projects/{project_id}/subscriptions/dashboard-sub"
dashboard_topic = f"projects/{project_id}/topics/dashboard-queue"

USER_MANAGEMENT_SERVICE_URL = os.getenv("USER_MANAGEMENT_SERVICE_URL")

# --- WebSocket Handlers ---
@socketio.on("join")
def on_join(data):
    logger.info(f"Client connected: {request.sid}")
    employee_id = data["employee_id"]
    auth_token = data["auth_token"]

    response = requests.get(
        f"{USER_MANAGEMENT_SERVICE_URL}/verify-token",
        headers={"Authorization": f"Bearer {auth_token}"}
    )

    if response.status_code == 200:
        join_room(employee_id)
        logger.info(f"User {employee_id} joined room with sid={request.sid}")
    else:
        logger.warning(f"Unauthorized join attempt for employee_id={employee_id}")
        disconnect()

@socketio.on("disconnect")
def on_disconnect():
    logger.info(f"User disconnected with sid={request.sid}")

@socketio.on("leave")
def on_leave(data):
    employee_id = data["employee_id"]
    leave_room(employee_id)
    logger.info(f"User {employee_id} left room")

# --- Pub/Sub Callback Function ---
def pubsub_callback(message):
    try:
        full_payload = json.loads(message.data.decode("utf-8"))
        logger.info(f"Pub/Sub message payload: {full_payload}")

        employee_id = full_payload.get("employee_id")
        message_type = full_payload.get("type", "generic")
        payload = {
            "type": message_type,
            "employee_id": employee_id,
            "data": full_payload.get("payload", full_payload),
        }

        # Emit the update to Socket.IO clients in the room identified by employee_id
        socketio.emit("dashboard_update", payload, room=employee_id)
        # Acknowledge the message
        subscriber.acknowledge(subscription_path, [message.ack_id])
        logger.info(f"Acknowledged message for employee_id={employee_id}")

    except Exception as e:
        logger.exception("Error processing Pub/Sub message")
        message.nack()

# --- Start the Pub/Sub Listener Asynchronously ---
def start_pubsub_listener():
    logger.info("Starting asynchronous Pub/Sub listener")
    future = subscriber.subscribe(subscription_path, callback=pubsub_callback)
    try:
        future.result()  # This blocks indefinitely unless an error occurs.
    except Exception as e:
        logger.exception("Error in asynchronous Pub/Sub listener: %s", e)

# Launch the listener as an eventlet green thread.
socketio.start_background_task(start_pubsub_listener)

# --- Note ---
# Do NOT call socketio.run() here; Gunicorn will run the app via your wsgi.py.


# --- Note ---
# Do NOT call socketio.run() here; Gunicorn will run the app via your wsgi.py.
