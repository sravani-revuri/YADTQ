import json
import redis
import signal
import sys
from kafka import KafkaConsumer

class Worker:
    def __init__(self, kafka_broker, redis_host, redis_port):
        self.kafka_topic = "task_queue"
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        self.running = True  # Flag to control loop

    def process_task(self, task):
        task_id = task["id"]
        task_type = task["type"]
        args = task["arguments"]

        self.redis_client.set(task_id, json.dumps({"status": "processing"}))
        try:
            if task_type == "add":
                result = sum(args)
                self.redis_client.set(task_id, json.dumps({"status": "completed", "result": result}))
                print(f" Task {task_id} completed. Result: {result}")
            else:
                raise ValueError(f"Unsupported task type: {task_type}")

        except Exception as e:
            self.redis_client.set(task_id, json.dumps({"status": "failed", "error": str(e)}))
            print(f" Task {task_id} failed: {str(e)}")

    def start(self):
        print("🚀 Worker started, waiting for tasks...")

        def shutdown_handler(sig, frame):
            """Handle Ctrl+C (SIGINT) for graceful shutdown."""
            print("\n Received shutdown signal. Stopping worker...")
            self.running = False
            self.consumer.close()
            sys.exit(0)

        # Register the signal handler
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        while self.running:
            for message in self.consumer:
                if not self.running:
                    break
                task = message.value
                print(f" Received task: {task}")
                self.process_task(task)

if __name__ == "__main__":
    worker = Worker(kafka_broker="localhost:9092", redis_host="localhost", redis_port=6379)
    worker.start()
