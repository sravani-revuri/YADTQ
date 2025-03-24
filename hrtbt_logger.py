import json
import time
import threading
import redis
from kafka import KafkaConsumer, KafkaProducer

class HrtBtLggr:
    def __init__(self, kafka_broker, redis_host="localhost", redis_port=6379, heartbeat_topic="heartbeat_queue"):
        self.heartbeat_topic = heartbeat_topic
        self.consumer = KafkaConsumer(
            self.heartbeat_topic,
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="heartbeat-monitor"
        )
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        self.worker_last_seen = {}
        self.running = True  # Flag to control shutdown

    def listen_for_heartbeats(self):
        """Continuously listen for worker heartbeats and update last seen timestamps."""
        with open("heartbeats.log", "a") as log_file:
            while self.running:
                for message in self.consumer:
                    if not self.running:
                        break
                    heartbeat = message.value
                    worker_id = heartbeat["worker_id"]
                    timestamp = heartbeat["timestamp"]

                    log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Worker {worker_id} is alive\n")
                    log_file.flush()

                    self.worker_last_seen[worker_id] = timestamp

    def reassign_stuck_tasks(self, worker_id):
        """Finds tasks assigned to a dead worker and resubmits them to Kafka."""
        with open("heartbeats.log", "a") as log_file:
            log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Reassigning tasks from failed worker {worker_id}...\n")
            log_file.flush()

        keys = self.redis_client.keys("*")  # Get all task keys
        for task_id in keys:
            task_data = self.redis_client.get(task_id)
            if not task_data:
                continue
            
            task_info = json.loads(task_data)

            # If task is still "processing" and assigned to the failed worker, reassign it
            if task_info.get("status") == "processing" and task_info.get("worker") == worker_id:
                with open("heartbeats.log", "a") as log_file:
                        log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Reassigning stuck task {task_id} from worker {worker_id}...\n")
                        log_file.flush()
                # Mark as "queued" again
                self.redis_client.set(task_id, json.dumps({"status": "queued"}), ex=300) 

                # Re-send the task to Kafka
                task_payload = {
                    "id": task_id,
                    "type": task_info["type"],
                    "arguments": task_info.get("arguments", [])
                }
                self.producer.send("task_queue", value=task_payload)
                self.producer.flush()
                with open("heartbeats.log", "a") as log_file:
                        log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Task {task_id} requeued successfully.\n")
                        log_file.flush()

    def check_unresponsive_workers(self):
        """Periodically checks if any workers have stopped sending heartbeats."""
        with open("heartbeats.log", "a") as log_file:
            while self.running:
                time.sleep(5)  # Check every 5 seconds
                current_time = time.time()
                for worker_id, last_seen in list(self.worker_last_seen.items()):
                    if current_time - last_seen > 10:  # Unresponsive for 10+ seconds
                        log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Worker {worker_id} is unresponsive! Reassigning tasks...\n")
                        log_file.flush()
                        
                        # Reassign tasks from failed worker
                        self.reassign_stuck_tasks(worker_id)

                        # Remove worker from tracking
                        del self.worker_last_seen[worker_id]

    def monitor_heartbeats(self):
        """Starts background threads to monitor worker heartbeats and detect failures."""
        self.listener_thread = threading.Thread(target=self.listen_for_heartbeats, daemon=True)
        self.check_thread = threading.Thread(target=self.check_unresponsive_workers, daemon=True)

        self.listener_thread.start()
        self.check_thread.start()

    def stop(self):
        """Stops heartbeat monitoring gracefully."""
        self.running = False
        self.consumer.close()
        print("[HeartbeatLogger] Stopped listening for heartbeats.")
