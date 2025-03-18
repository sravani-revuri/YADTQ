import json
import redis
import time
from kafka import KafkaConsumer

# Kafka and Redis Configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "task_queue"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
WORKER_ID = "worker-1"

# Initialize Redis
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="worker-group",
    auto_offset_reset="earliest",
)


def process_task(task):
    """Execute the task based on its type."""
    task_id = task["task-id"]
    task_type = task["task"]
    args = task["args"]

    redis_client.set(task_id, json.dumps({"status": "processing"}))

    try:
        if task_type == "add":
            result = sum(args)
        else:
            raise ValueError("Unknown task type")

        # Update status in Redis
        redis_client.set(task_id, json.dumps({"status": "success", "result": result}))
        print(f"Task {task_id} completed successfully with result: {result}")

    except Exception as e:
        redis_client.set(task_id, json.dumps({"status": "failed", "error": str(e)}))
        print(f"Task {task_id} failed: {str(e)}")


def send_heartbeat():
    """Send worker heartbeat to Redis."""
    redis_client.set(f"heartbeat:{WORKER_ID}", time.time())


# Main worker loop
if __name__ == "__main__":
    print("Worker started...")

    while True:
        send_heartbeat()  # Send heartbeat periodically

        for message in consumer:
            task = message.value
            print(f"Received task: {task}")
            process_task(task)
