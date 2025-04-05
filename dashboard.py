import json
from flask import Flask, request
from flask_socketio import SocketIO, join_room, leave_room, disconnect
import threading
import os
import requests
from google.cloud import pubsub_v1

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

    # Verify token with user management service
    response = requests.get(f'{USER_MANAGEMENT_SERVICE_URL}/verify-token', headers={
        'Authorization': f'Bearer {auth_token}'
    })

    if response.status_code == 200:
        sid = request.sid
        join_room(employee_id)
        print(f"User {employee_id} joined room with sid={sid}")
    else:
        disconnect()

@socketio.on('disconnect')
def on_disconnect():
    sid = request.sid
    print(f"User disconnected with sid={sid}")
    # No need for Redis cleanup as we're not using Redis anymore.

@socketio.on('leave')
def on_leave(data):
    employee_id = data['employee_id']
    leave_room(employee_id)
    print(f"User {employee_id} left room")


# --- Pub/Sub Consumer Thread ---
def listen_pubsub_messages():
    while True:
        # Pull messages from the Pub/Sub subscription
        subscription_path = subscriber.subscription_path('hopkinstimesheetproj', dashboard_subscription)
        response = subscriber.pull(subscription_path, max_messages=1, return_immediately=True)

        if response.received_messages:
            for msg in response.received_messages:
                full_payload = json.loads(msg.message.data.decode("utf-8"))
                employee_id = full_payload.get('employee_id')
                message_type = full_payload.get('type', 'generic')
                payload = {
                    "type": message_type,
                    "employee_id": employee_id,
                    "data": full_payload.get('payload', full_payload)
                }

                # Emit the message to the appropriate room
                socketio.emit("dashboard_update", payload, room=employee_id)

                # Acknowledge the message after processing
                subscriber.acknowledge(subscription_path, [msg.ack_id])


if __name__ == '__main__':
    threading.Thread(target=listen_pubsub_messages, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=8000, use_reloader=False)
