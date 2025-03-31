from flask import Flask, request
from flask_socketio import SocketIO, join_room, leave_room
import boto3
import json
import threading
import os
import requests

app = Flask(__name__)
socketio = SocketIO(app)

sqs = boto3.client('sqs')
dashboard_queue_url = sqs.get_queue_url(QueueName='dashboard_queue')['QueueUrl']
USER_MANAGEMENT_SERVICE_URL = os.getenv('USER_MANAGEMENT_SERVICE_URL')


@socketio.on('join')
def on_join(data):
    employee_id = data['employee_id']
    auth_token = data['auth_token']  # Get the auth token from the client

    # Verify the token with User Management Service
    response = requests.get(f'{USER_MANAGEMENT_SERVICE_URL}/verify-token', headers={
        'Authorization': f'Bearer {auth_token}'
    })

    if response.status_code == 200:
        # The token is valid, join the room
        join_room(employee_id)


@socketio.on('leave')
def on_leave(data):
    employee_id = data['employee_id']
    # Likewise, when they leave (log out, etc.), they leave their given room.
    leave_room(employee_id)


def listen_sqs_messages():
    while True:
        messages = sqs.receive_message(
            QueueUrl=dashboard_queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=30,
            WaitTimeSeconds=20
        )

        if 'Messages' in messages:
            for message in messages['Messages']:
                dashboard_data = json.loads(message['Body'])
                print(f"Dashboard data received: {dashboard_data}")

                # Parse data to find the employee id
                employee_id = dashboard_data['employee_id']

                # Emit the data to the room associated with a specific employee
                socketio.emit('dashboard_update', dashboard_data, room=employee_id)

                sqs.delete_message(
                    QueueUrl=dashboard_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )


if __name__ == '__main__':
    sqs_thread = threading.Thread(target=listen_sqs_messages)
    sqs_thread.start()

    socketio.run(app)
