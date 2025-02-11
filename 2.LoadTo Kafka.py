import json
import time
from kafka import KafkaProducer
import configuration as c

# Kafka Configuration
# KAFKA_BROKER = "course-kafka:9092" 
# KAFKA_TOPIC = 'Customer_Support'  

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=c.KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

# Load JSON data from file
json_file_path = c.json_file_path
with open(json_file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)

# Ensure data is a list of JSON objects
if isinstance(data, dict):  # If single object, wrap in a list
    data = [data]

# Send messages to Kafka every 2 seconds
for record in data:
    try:
        producer.send(c.KAFKA_TOPIC, key=record.get("Unique id", ""), value=record)
        producer.flush()
        print("Message sent to Kafka:", json.dumps(record))
        time.sleep(2)
    except KeyboardInterrupt:
        print("Stopping producer...")
        break
    except Exception as e:
        print("Error sending message to Kafka:", str(e))
