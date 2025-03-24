import json
import time
import threading
from kafka import KafkaConsumer

class HrtBtLggr:
    def __init__(self, kafka_broker, heartbeat_topic="heartbeat_queue"):
        self.heartbeat_topic = heartbeat_topic
        self.consumer = KafkaConsumer(
            self.heartbeat_topic,
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="heartbeat-monitor"
        )
        self.worker_last_seen = {}
        self.running = True  # Flag to control shutdown

    def listen_for_heartbeats(self):
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

    def check_unresponsive_workers(self):
        with open("heartbeats.log", "a") as log_file:
            while self.running:
                time.sleep(5)  # Check every 5 seconds
                current_time = time.time()
                for worker_id, last_seen in list(self.worker_last_seen.items()):
                    if current_time - last_seen > 10:
                        log_file.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Worker {worker_id} is unresponsive!\n")
                        log_file.flush()
                        del self.worker_last_seen[worker_id]

    def monitor_heartbeats(self):
        self.listener_thread = threading.Thread(target=self.listen_for_heartbeats, daemon=True)
        self.check_thread = threading.Thread(target=self.check_unresponsive_workers, daemon=True)

        self.listener_thread.start()
        self.check_thread.start()

    def stop(self):
        self.running = False
        self.consumer.close()
        print("[HeartbeatLogger] Stopped listening for heartbeats.")
