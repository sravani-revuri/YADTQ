import json
import uuid
import redis
import itertools
import threading
import time
from kafka import KafkaProducer,KafkaConsumer

class YADTQ:
    def __init__(self, kafka_broker, redis_host, redis_port, partitions=3):
        self.kafka_topic = "task_queue"
        self.heartbeat_topic = "heartbeat_queue"

        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.partition_cycle = itertools.cycle(range(partitions))

        self.worker_last_seen = {}  # Track last heartbeat time
        self.heartbeat_thread = threading.Thread(target=self.monitor_heartbeats, daemon=True)
        self.heartbeat_thread.start()

    def monitor_heartbeats(self):
        self.consumer = KafkaConsumer(
            self.heartbeat_topic,
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="heartbeat-monitor"
        )

        with open("heartbeats.log", "a") as log_file:
            while True:
                for message in self.consumer:
                    heartbeat = message.value
                    worker_id = heartbeat["worker_id"]
                    timestamp = heartbeat["timestamp"]

                    # Log the heartbeat
                    log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Worker {worker_id} is alive\n")
                    log_file.flush()

                    # Update last seen time
                    self.worker_last_seen[worker_id] = timestamp

                # Check for failures (workers that haven't sent heartbeat in 10 seconds)
                current_time = time.time()
                for worker_id, last_seen in list(self.worker_last_seen.items()):
                    if current_time - last_seen > 10:
                        log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Worker {worker_id} is unresponsive!\n")
                        log_file.flush()
                        del self.worker_last_seen[worker_id]

    def submit_task(self, task_type, args):
        task_id = str(uuid.uuid4())
        task_data = {
            "id": task_id,
            "type": task_type,
            "arguments": args  
        }
        partition = next(self.partition_cycle)
        # Set initial task status with TTL (avoids stuck queued tasks)
        self.redis_client.set(task_id, json.dumps({"status": "queued"}), ex=300)  # 5 min TTL

        try:
            future = self.producer.send(self.kafka_topic, value=task_data, partition=partition)
            future.get(timeout=10)
            self.producer.flush()
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
        print("\nClosing Kafka producer and Redis connection...")
        self.producer.close()
        print("Resources released. Exiting safely.")
