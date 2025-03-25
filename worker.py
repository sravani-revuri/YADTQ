import json
import redis
import signal
import time
import sys
import threading
from functools import reduce
import math
from kafka import KafkaConsumer,KafkaProducer

class Worker:
    def __init__(self, kafka_broker, redis_host, redis_port, worker_id):
        self.kafka_topic = "task_queue"
        self.heartbeat_topic = "heartbeat_queue"
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="worker-group"  # Consumer Group for Load Balancing
        )
    
        self.producer=KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self.worker_id = worker_id
        self.running = True  # Flag to control loop
        self.task_queue = [] # Track the current task

    def send_heartbeat(self):
        while self.running:
            hrtbt_msg={"worker_id": self.worker_id,
                "timestamp": time.time(),
                "tasks": self.task_queue.copy()  }
            self.producer.send(self.heartbeat_topic, value=hrtbt_msg)
            self.producer.flush()
            self.task_queue.clear()
            time.sleep(5)

    def process_task(self, task):
        task_id = task["id"]
        task_type = task["type"]
        args = task["arguments"]

        self.current_task = {"task_id": task_id, "type": task_type, "arguments": args}  # Store for heartbeat

        self.redis_client.set(task_id, json.dumps({
        "status": "processing",
        "worker": self.worker_id,
        "type": task_type,         # Store task type
        "arguments": args          # Store arguments
    }))
        try:
            if task_type == "add":
                time.sleep(30)
                result = sum(args)
                self.redis_client.set(task_id, json.dumps({"status": "completed", "result": result, "worker": self.worker_id}))
                print(f"[Worker {self.worker_id}] Task {task_id} completed. Result: {result}")


            elif task_type == "sub":
                time.sleep(1)
                result = reduce(lambda x, y: x - y, args)
                self.redis_client.set(task_id, json.dumps({"status": "completed", "result": result, "worker": self.worker_id}))
                print(f"[Worker {self.worker_id}] Task {task_id} completed. Result: {result}")


            elif task_type == "mul":
                time.sleep(1)
                result=math.prod(args)
                self.redis_client.set(task_id, json.dumps({"status": "completed", "result": result, "worker": self.worker_id}))
                print(f"[Worker {self.worker_id}] Task {task_id} completed. Result: {result}")

            else:
                raise ValueError(f"Unsupported task type: {task_type}")

        except Exception as e:
            self.redis_client.set(task_id, json.dumps({"status": "failed", "error": str(e), "worker": self.worker_id}))
            print(f"[Worker {self.worker_id}] Task {task_id} failed: {str(e)}")

        finally:
            self.current_task = None


    def start(self):
        print(f"[Worker {self.worker_id}] Started, waiting for tasks...")

        def shutdown_handler(sig, frame):
            """Handle Ctrl+C (SIGINT) for graceful shutdown."""
            print(f"\n[Worker {self.worker_id}] Received shutdown signal. Stopping...")
            self.running = False
            self.consumer.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        heartbeat_thread.start()

        while self.running:
            for message in self.consumer:
                if not self.running:
                    break
                task = message.value
                print(f"[Worker {self.worker_id}] Received task: {task}")
                self.process_task(task)



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", type=str, required=True, help="Unique Worker ID")
    args = parser.parse_args()

    worker = Worker(kafka_broker="localhost:9092", redis_host="localhost", redis_port=6379, worker_id=args.worker_id)
    worker.start()
