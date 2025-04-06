import json
import threading
import time
import os
import requests
import logging

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
socketio = SocketIO(app, cors_allowed_origins="*", path="/api/dashboard/socket.io")

# --- Google Pub/Sub Setup ---
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

project_id = "hopkinstimesheetproj"
dashboard_subscription = f"projects/{project_id}/subscriptions/dashboard-sub"
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
        sid = request.sid
        join_room(employee_id)
        logger.info(f"User {employee_id} joined room with sid={sid}")
    else:
        logger.warning(f"Unauthorized join attempt for employee_id={employee_id}")
        disconnect()


@socketio.on("disconnect")
def on_disconnect():
    sid = request.sid
    logger.info(f"User disconnected with sid={sid}")


@socketio.on("leave")
def on_leave(data):
    employee_id = data["employee_id"]
    leave_room(employee_id)
    logger.info(f"User {employee_id} left room")


# --- Pub/Sub Listener Thread ---
def listen_pubsub_messages():
    logger.info("Dashboard Pub/Sub listener thread started.")
    subscription_path = dashboard_subscription

    while True:
        try:
            response = subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": 1,
                }
            )
            if not response.received_messages:
                time.sleep(2)

            if response.received_messages:
                logger.info(f"Received {len(response.received_messages)} message(s) from Pub/Sub.")
                for msg in response.received_messages:
                    try:
                        full_payload = json.loads(msg.message.data.decode("utf-8"))
                        logger.info(f"Pub/Sub message payload: {full_payload}")

                        employee_id = full_payload.get("employee_id")
                        message_type = full_payload.get("type", "generic")
                        payload = {
                            "type": message_type,
                            "employee_id": employee_id,
                            "data": full_payload.get("payload", full_payload),
                        }

                        socketio.emit("dashboard_update", payload, room=employee_id)
                        subscriber.acknowledge(subscription_path, [msg.ack_id])
                        logger.info(f"Acknowledged message for employee_id={employee_id}")

                    except Exception as e:
                        logger.exception("Error processing Pub/Sub message")
            else:
                time.sleep(2)

        except Exception as e:
            logger.exception("Error in Pub/Sub listener loop")


# --- Start Pub/Sub Thread Immediately ---
threading.Thread(target=listen_pubsub_messages, daemon=True).start()

# --- SocketIO Server ---
# Do NOT run socketio.run() here; Gunicorn will run the app via wsgi.py
