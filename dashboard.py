import json
from flask import Flask, request
from flask_socketio import SocketIO, join_room, leave_room, disconnect
import threading
import os
import requests
import time
from google.cloud import pubsub_v1

from google.cloud import logging as cloud_logging
import logging

# Set up Google Cloud Logging client
cloud_log_client = cloud_logging.Client()
cloud_log_client.setup_logging()

# Optional: Standard Python logger for local/dev use too
logger = logging.getLogger("dashboard_service")
logger.setLevel(logging.INFO)

app = Flask(__name__)
# Initialize SocketIO without Redis
socketio = SocketIO(app, cors_allowed_origins="*", path='/api/dashboard/socket.io')

# Google Cloud Pub/Sub client initialization
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

# GCP Pub/Sub subscription and topic names (replace with actual values)
dashboard_topic = 'projects/hopkinstimesheetproj/topics/dashboard-queue'
dashboard_subscription = 'projects/hopkinstimesheetproj/subscriptions/dashboard-sub'

USER_MANAGEMENT_SERVICE_URL = os.getenv('USER_MANAGEMENT_SERVICE_URL')


@socketio.on('join')
def on_join(data):
    employee_id = data['employee_id']
    auth_token = data['auth_token']

    response = requests.get(f'{USER_MANAGEMENT_SERVICE_URL}/verify-token', headers={
        'Authorization': f'Bearer {auth_token}'
    })

    if response.status_code == 200:
        sid = request.sid
        join_room(employee_id)
        logger.info(f"User {employee_id} joined room with sid={sid}")
    else:
        logger.warning(f"Unauthorized join attempt for employee_id={employee_id}")
        disconnect()

@socketio.on('disconnect')
def on_disconnect():
    sid = request.sid
    logger.info(f"User disconnected with sid={sid}")

@socketio.on('leave')
def on_leave(data):
    employee_id = data['employee_id']
    leave_room(employee_id)
    logger.info(f"User {employee_id} left room")


# --- Pub/Sub Consumer Thread ---
def listen_pubsub_messages():
    logger.info("Dashboard Pub/Sub listener started.")
    subscription_path = subscriber.subscription_path('hopkinstimesheetproj', dashboard_subscription)

    while True:
        response = subscriber.pull(subscription_path, max_messages=1, return_immediately=True)

        if response.received_messages:
            logger.info(f"Received {len(response.received_messages)} message(s) from Pub/Sub.")
            for msg in response.received_messages:
                try:
                    full_payload = json.loads(msg.message.data.decode("utf-8"))
                    logger.info(f"Pub/Sub message payload: {full_payload}")

                    employee_id = full_payload.get('employee_id')
                    message_type = full_payload.get('type', 'generic')
                    payload = {
                        "type": message_type,
                        "employee_id": employee_id,
                        "data": full_payload.get('payload', full_payload)
                    }

                    socketio.emit("dashboard_update", payload, room=employee_id)
                    subscriber.acknowledge(subscription_path, [msg.ack_id])
                    logger.info(f"Acknowledged message for employee_id={employee_id}")

                except Exception as e:
                    logger.exception("Error processing Pub/Sub message")
        else:
            logger.debug("No messages in Pub/Sub queue.")
            time.sleep(2)


if __name__ == '__main__':
    threading.Thread(target=listen_pubsub_messages, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=8000, use_reloader=False)
