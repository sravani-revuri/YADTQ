import json
import time
from kafka import KafkaConsumer

class TaskLogger:
    def __init__(self, kafka_broker):
        self.kafka_topic = "task_queue"  # Listens to task assignments
        self.consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=kafka_broker,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="task-logger"
        )
        self.running = True  # Flag to control shutdown

    def log_tasks(self):
        """Continuously listen for new tasks and log them."""
        with open("worker.log", "a") as log_file:
            while self.running:
                for message in self.consumer:
                    if not self.running:
                        break
                    task = message.value
                    task_id = task["id"]
                    task_type = task["type"]
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

                    log_entry = f"[{timestamp}] Task {task_id} assigned: {task_type}\n"
                    log_file.write(log_entry)
                    log_file.flush()  # Ensure it writes immediately

    def stop(self):
        """Stop the logger cleanly."""
        self.running = False
        self.consumer.close()
        print("[TaskLogger] Stopped logging tasks.")
