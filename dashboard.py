import json
from flask import Flask, request
from flask_socketio import SocketIO, join_room, leave_room, disconnect
import threading
import os
import requests
import redis
from google.cloud import pubsub_v1

# Redis and Flask setup
REDIS_HOST = os.getenv('REDIS_HOST', 'your-memorystore-instance-ip')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
POD_ID = os.getenv("POD_ID", os.urandom(4).hex())
REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/0" if REDIS_PASSWORD else f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

app = Flask(__name__)
# Pass the custom path '/api/dashboard/socket.io' for SocketIO
socketio = SocketIO(app, cors_allowed_origins="*", message_queue=REDIS_URL, path='/api/dashboard/socket.io')
r = redis.Redis.from_url(REDIS_URL)

# Google Cloud Pub/Sub client initialization
subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

# GCP Pub/Sub subscription and topic names (replace with actual values)
dashboard_topic = 'projects/hopkinstimesheetproj/topics/dashboard-topic'
dashboard_subscription = 'projects/hopkinstimesheetproj/subscriptions/dashboard-subscription'

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
        # Store mapping in Redis (for multi-pod routing)
        r.set(f"user:{employee_id}", f"{POD_ID}:{sid}", ex=3600)
        print(f"[{POD_ID}] User {employee_id} joined room with sid={sid}")
    else:
        disconnect()

@socketio.on('disconnect')
def on_disconnect():
    sid = request.sid
    for key in r.scan_iter("user:*"):
        if r.get(key).decode().endswith(f":{sid}"):
            r.delete(key)
            break

@socketio.on('leave')
def on_leave(data):
    employee_id = data['employee_id']
    leave_room(employee_id)
    r.delete(f"user:{employee_id}")


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

                # Get the user’s session id from Redis
                redis_val = r.get(f"user:{employee_id}")
                if redis_val:
                    target_pod, sid = redis_val.decode().split(":")
                    if target_pod == POD_ID:
                        print(f"[{POD_ID}] Emitting locally to {employee_id}")
                        socketio.emit("dashboard_update", payload, room=employee_id)
                    else:
                        print(f"[{POD_ID}] Publishing to Redis for {employee_id} on pod {target_pod}")
                        r.publish("dashboard-updates", json.dumps({
                            "employee_id": employee_id,
                            "type": message_type,
                            "payload": payload["data"]
                        }))

                # Acknowledge the message after processing
                subscriber.acknowledge(subscription_path, [msg.ack_id])


def listen_redis_pubsub():
    pubsub = r.pubsub()
    pubsub.subscribe("dashboard-updates")
    for message in pubsub.listen():
        if message['type'] != 'message':
            continue
        try:
            data = json.loads(message['data'])
            employee_id = data['employee_id']
            payload = {
                "type": data.get("type", "generic"),
                "employee_id": employee_id,
                "data": data.get("payload", {})
            }

            sid = r.get(f"user:{employee_id}")
            if sid and sid.decode().startswith(POD_ID):
                print(f"[{POD_ID}] Redis pubsub: emitting to {employee_id}")
                socketio.emit("dashboard_update", payload, room=employee_id)

        except Exception as e:
            print(f"Redis pubsub error: {e}")


if __name__ == '__main__':
    threading.Thread(target=listen_pubsub_messages, daemon=True).start()
    threading.Thread(target=listen_redis_pubsub, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=8000, use_reloader=False)
