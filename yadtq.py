import json
import uuid
import redis
import time
import signal
import sys
from kafka import KafkaProducer

class YADTQ():
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

            print(f" Task {task_id} sent to Kafka with queued status")
        except Exception as e:
            print(f" Error submitting Task {task_id} to Kafka: {str(e)}")
            self.redis_client.set(task_id, json.dumps({"status": "failed"}))
            return None

        return task_id

    def get_status(self, task_id):
        task_info = self.redis_client.get(task_id)
        return json.loads(task_info) if task_info else {"error": "Task ID not found"}

    def close(self):
        """Closes Kafka producer and Redis connection."""
        print("\n Closing Kafka producer and Redis connection...")
        self.producer.close()
        print(" Resources released. Exiting safely.")

def graceful_exit(signum, frame):
    """Handle shutdown signals (`SIGINT`, `SIGTERM`) gracefully."""
    print("\n Graceful shutdown detected. Exiting...")
    task_manager.close()
    sys.exit(0)

def main():
    global task_manager
    task_manager = YADTQ(kafka_broker="localhost:9092", redis_host="localhost", redis_port=6379)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, graceful_exit)
    signal.signal(signal.SIGTERM, graceful_exit)

    try:
        while True:
            print("\n Choose an option:")
            print("1️. Submit a new task")
            print("2️.  Check task status")
            print("3️.  Exit")
            choice = input("Enter choice (1/2/3): ").strip()

            if choice == "1":
                task_type = input("Enter task type (e.g., add): ").strip()
                args = input("Enter arguments (comma-separated numbers): ").strip()
                args_list = list(map(int, args.split(",")))

                task_id = task_manager.submit_task(task_type, args_list)
                if task_id:
                    print(f" Submitted Task ID: {task_id}")

            elif choice == "2":
                task_id = input("Enter Task ID to check status: ").strip()
                status = task_manager.get_status(task_id)
                print(f" Task Status: {status}")

            elif choice == "3":
                print(" Exiting YADTQ...")
                task_manager.close()
                break

            else:
                print("⚠️ Invalid choice! Please select 1, 2, or 3.")

            time.sleep(1)

    except KeyboardInterrupt:
        graceful_exit(None, None)

if __name__ == "__main__":
    main()
