import json
import uuid
import redis
from kafka import KafkaProducer

class YADTQ:
    def __init__(self, kafka_broker, redis_host, redis_port):
        self.kafka_topic = "task_queue"
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def submit_task(self, task_type, args):
        task_id = str(uuid.uuid4())
        task_data = {
            "id": task_id,
            "type": task_type,
            "arguments": args  
        }

        # Set initial task status in Redis
        self.redis_client.set(task_id, json.dumps({"status": "queued"}))

        try:
            # Send task to Kafka and wait for confirmation
            future = self.producer.send(self.kafka_topic, task_data)
            future.get(timeout=10)  # Blocks until Kafka confirms delivery
            self.producer.flush()   # Ensure the message is sent

            print(f"Task {task_id} sent to Kafka with queued status")
        except Exception as e:
            print(f"Error submitting Task {task_id} to Kafka: {str(e)}")
            self.redis_client.set(task_id, json.dumps({"status": "failed"}))
            return None

        return task_id

    def get_status(self, task_id):
        task_info = self.redis_client.get(task_id)
        return json.loads(task_info) if task_info else {"error": "Task ID not found"}

    def close(self):
        """Closes Kafka producer and Redis connection."""
        print("\nClosing Kafka producer and Redis connection...")
        self.producer.close()
        print("Resources released. Exiting safely.")
