import json
import uuid
import redis
import itertools
import threading
from hrtbt_logger import HrtBtLggr
from kafka import KafkaProducer
from task_logger import TaskLogger

class YADTQ:
    def __init__(self, kafka_broker, redis_host, redis_port, partitions=3):
        self.kafka_topic = "task_queue"
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        self.partition_cycle = itertools.cycle(range(partitions))

        # Start heartbeat logger in a background thread
        self.heartbeat_logger = HrtBtLggr(kafka_broker)
        self.heartbeat_logger.monitor_heartbeats()

        self.task_logger = TaskLogger(kafka_broker)
        self.task_logger_thread = threading.Thread(target=self.task_logger.log_tasks, daemon=True)
        self.task_logger_thread.start()

    def submit_task(self, task_type, args):
        task_id = str(uuid.uuid4())
        partition = next(self.partition_cycle)
        task_data = {
            "id": task_id,
            "type": task_type,
            "arguments": args  
        }

    # Set initial task status in Redis with TTL (avoids stuck queued tasks)
        self.redis_client.set(task_id, json.dumps({"status": "queued"}), ex=300)  # 5 min TTL

        try:
            # Let Kafka auto-assign partition
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
        """Gracefully shuts down Kafka producer and heartbeat logger."""
        print("\nClosing Kafka producer and Redis connection...")
        self.producer.close()
        self.heartbeat_logger.stop()  # Stop heartbeat logger
        self.task_logger.stop()
        print("Resources released. Exiting safely.")
