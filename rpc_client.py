# import pika
# import random
# import json
# import time

# class MqtTClient:
#     def __init__(self, host='localhost', queue='mqtt_queue'):
#         self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
#         self.channel = self.connection.channel()
#         self.queue = queue
#         self.channel.queue_declare(queue=self.queue)

#     def publish_status(self):
#         try:
#             while True:
#                 status = random.randint(0, 6)
#                 message = {
#                     'status': status,
#                     'timestamp': time.time()
#                 }
#                 self.channel.basic_publish(
#                     exchange='',
#                     routing_key=self.queue,
#                     body=json.dumps(message)
#                 )
#                 print(f"Published: {message}")
#                 time.sleep(1)
#         except KeyboardInterrupt:
#             print("Interrupted by user")

#     def close(self):
#         if self.connection:
#             self.connection.close()

# if __name__ == "__main__":
#     client = MqtTClient()
#     client.publish_status()
#     client.close()



# client.py

# import pika
# import random
# import json
# import time

# class MqttClient:
#     def __init__(self, host='localhost', queue='mqtt_queue'):
#         self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
#         self.channel = self.connection.channel()
#         self.queue = queue
#         self.channel.queue_declare(queue=self.queue)

#     def publish_status(self):
#         try:
#             while True:
#                 status = random.randint(0, 6)
#                 print("print time",time.time())
#                 message = {
#                     'status': status,
#                     'timestamp': time.time()
#                 }
#                 self.channel.basic_publish(
#                     exchange='',
#                     routing_key=self.queue,
#                     body=json.dumps(message)
#                 )
#                 print(f"Published: {message}")
#                 time.sleep(1)  # Adjust the sleep time as needed
#         except KeyboardInterrupt:
#             print("Interrupted by user")

#     def close(self):
#         if self.connection:
#             self.connection.close()

# if __name__ == "__main__":
#     client = MqttClient()
#     client.publish_status()
#     client.close()



import pika
import random
import json
import time
from datetime import datetime
import pytz

class MqttClient:
    def __init__(self, host='localhost', queue='mqtt_queue'):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.queue = queue
        self.channel.queue_declare(queue=self.queue)

    def publish_status(self):
        try:
            while True:
                status = random.randint(0, 6)
                # Current time in UTC
                timestamp = time.time()
                # utc_dt = datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.utc)
                print("printing",timestamp)
                message = {
                    'status': status,
                    'timestamp': timestamp  # Store in UTC
                }
                self.channel.basic_publish(
                    exchange='',
                    routing_key=self.queue,
                    body=json.dumps(message)
                )
                print(f"Published: {message}")
                time.sleep(1)  # Adjust the sleep time as needed
        except KeyboardInterrupt:
            print("Interrupted by user")

    def close(self):
        if self.connection:
            self.connection.close()

if __name__ == "__main__":
    client = MqttClient()
    client.publish_status()
    client.close()
