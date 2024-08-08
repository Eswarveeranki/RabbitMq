# import pika
# import json
# from pymongo import MongoClient
# from flask import Flask, request, jsonify

# from datetime import datetime


# app = Flask(__name__)

# # Setup MongoDB
# mongo_client = MongoClient('mongodb://localhost:27017/')
# db = mongo_client['mqtt_data']
# collection = db['statuses']

# def handle_message(ch, method, properties, body):
#     try:
#         message = json.loads(body)
#         collection.insert_one(message)
#         print(f"Stored in MongoDB: {message}")
#         ch.basic_ack(delivery_tag=method.delivery_tag)
#     except Exception as e:
#         print(f"Error storing message: {e}")

# # Setup RabbitMQ
# def setup_rabbitmq():
#     connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
#     channel = connection.channel()
#     channel.queue_declare(queue='mqtt_queue')
#     channel.basic_qos(prefetch_count=1)
#     channel.basic_consume(queue='mqtt_queue', on_message_callback=handle_message)
#     print("Waiting for messages...")
#     channel.start_consuming()

# @app.route('/status_count', methods=['GET'])
# def get_status_count():
#     try:
#         # Get datetime strings from the request
#         start_time_str = request.args.get('start')
#         end_time_str = request.args.get('end')

#         # Convert datetime strings to Unix timestamps
#         start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M").timestamp()
#         end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M").timestamp()

#         # MongoDB aggregation pipeline
#         pipeline = [
#             {"$match": {"timestamp": {"$gte": start_time, "$lte": end_time}}},
#             {"$group": {"_id": "$status", "count": {"$sum": 1}}}
#         ]
#         result = list(collection.aggregate(pipeline))
#         return jsonify(result)
#     except Exception as e:
#         return jsonify({"error": str(e)}), 400

# if __name__ == "__main__":
#     from threading import Thread
#     # Start RabbitMQ in a separate thread
#     rabbitmq_thread = Thread(target=setup_rabbitmq, daemon=True)
#     rabbitmq_thread.start()

#     # Start the Flask server
#     app.run(host='0.0.0.0', port=5000)



import pika
import json
import psycopg2
from flask import Flask, request, jsonify
from datetime import datetime
from threading import Thread

app = Flask(__name__)

# Database connection parameters
DB_HOST = "DB_HOST"
DB_NAME = "DB_NAME"
DB_USER = "DB_USER"
DB_PASSWORD = "DB_PASSWORD"

def get_db_connection():
	"""Create a new database connection."""
	return psycopg2.connect(
		host=DB_HOST,
		database=DB_NAME,
		user=DB_USER,
		password=DB_PASSWORD
	)

def handle_message(ch, method, properties, body):
	"""Handle incoming messages from RabbitMQ."""
	try:
		message = json.loads(body)
		print("message",message)
		status = message['status']
		print("status",status)
		timestamp = int(message['timestamp'])  # Use integer timestamp for storage
		print("printing the data on the line no :97",timestamp)
		conn = get_db_connection()
		cursor = conn.cursor()
		cursor.execute("INSERT INTO statuses (status, timestamp) VALUES (%s, %s)", (status, timestamp))
		conn.commit()

		cursor.close()
		conn.close()

		print(f"Stored in PostgreSQL: {message}")
		ch.basic_ack(delivery_tag=method.delivery_tag)
	except Exception as e:
		print(f"Error storing message: {e}")

# Setup RabbitMQ
def setup_rabbitmq():
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()
	channel.queue_declare(queue='mqtt_queue')
	channel.basic_qos(prefetch_count=1)
	channel.basic_consume(queue='mqtt_queue', on_message_callback=handle_message)
	print("Waiting for messages...")
	channel.start_consuming()

@app.route('/status_count', methods=['GET'])
def get_status_count():
	"""API endpoint to get status counts within a time range."""
	try:
		# Get datetime strings from the request
		start_time_str = request.args.get('start')
		end_time_str = request.args.get('end')

		# Convert datetime strings to Unix timestamps
		start_time = int(datetime.strptime(start_time_str, "%Y-%m-%d %H:%M").timestamp())
		end_time = int(datetime.strptime(end_time_str, "%Y-%m-%d %H:%M").timestamp())
		 
		print(f"Querying between {start_time} and {end_time}")

		# PostgreSQL query to count statuses within the time range
		conn = get_db_connection()
		cursor = conn.cursor()
		cursor.execute("""SELECT status, COUNT(*)FROM statusesWHERE timestamp BETWEEN %s AND %sGROUP BY status""", (start_time, end_time))
		result = cursor.fetchall()
		cursor.close()
		conn.close()
		print(f"Query Result: {result}")
		return jsonify([{str(row[0]): row[1]} for row in result])
	except Exception as e:
		return jsonify({"error": str(e)}), 400

if __name__ == "__main__":
	# Start RabbitMQ in a separate thread
	rabbitmq_thread = Thread(target=setup_rabbitmq, daemon=True)
	rabbitmq_thread.start()

	# Start the Flask server
	app.run(host='0.0.0.0', port=5000)
