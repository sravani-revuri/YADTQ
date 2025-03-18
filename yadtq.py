import json
import uuid
import redis
from kafka import KafkaProducer

# Kafka and Redis Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "task_queue"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Initialize Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def submit_task(task_type, args):
    """Submit a task to Kafka and track it in Redis."""
    task_id = str(uuid.uuid4())
    task_data = {"task-id": task_id, "task": task_type, "args": args}

    # Store initial task status in Redis
    redis_client.set(task_id, json.dumps({"status": "queued"}))

    try:
        # Send task to Kafka
        future = producer.send(KAFKA_TOPIC, task_data)
        producer.flush()  # Ensure the message is sent immediately

        # Confirm the message was successfully sent
        future.get(timeout=10)  # Raises an error if send fails
        print(f"✅ Task {task_id} submitted successfully.")

    except Exception as e:
        print(f"❌ Error submitting task {task_id}: {e}")

    return task_id


def get_task_status(task_id):
    """Retrieve the task status from Redis."""
    task_info = redis_client.get(task_id)
    if task_info:
        return json.loads(task_info)
    return {"error": "Task ID not found"}


# Example Usage
if __name__ == "__main__":
    task_id = submit_task("add", [5, 3])
    print(f"Submitted Task ID: {task_id}")
    print("Checking status:", get_task_status(task_id))
