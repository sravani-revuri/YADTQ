import signal
import sys
from yadtq import YADTQ

class Client:
    def __init__(self, kafka_broker="localhost:9092", redis_host="localhost", redis_port=6379):
        self.task_manager = YADTQ(kafka_broker, redis_host, redis_port)

    def submit_task(self, task_type, args):
        task_id = self.task_manager.submit_task(task_type, args)
        if task_id:
            print(f"Task submitted successfully! Task ID: {task_id}")
        else:
            print("Task submission failed.")
        return task_id

    def check_status(self, task_id):
        """Checks and prints the status of a given task."""
        status = self.task_manager.get_status(task_id)
        print(f"Task Status: {status}")
        return status

    def close(self):
        """Closes the task manager resources."""
        self.task_manager.close()

# Handle graceful exit
def graceful_exit(signum, frame):
    print("\nGraceful shutdown detected. Exiting...")
    client.close()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

if __name__ == "__main__":
    client = Client()

    while True:
        print("\nClient Menu:")
        print("1. Submit a new task")
        print("2. Check task status")
        print("3. Exit")
        choice = input("Enter choice (1/2/3): ").strip()

        if choice == "1":
            task_type = input("Enter task type add ,sub ,mul ").strip()
            args = input("Enter arguments (comma-separated numbers): ").strip()
            try:
                args_list = list(map(int, args.split(",")))
                client.submit_task(task_type, args_list)
            except ValueError:
                print("Invalid input! Please enter numbers separated by commas.")

        elif choice == "2":
            task_id = input("Enter Task ID to check status: ").strip()
            client.check_status(task_id)

        elif choice == "3":
            print("Exiting client...")
            client.close()
            break

        else:
            print("Invalid choice! Please select 1, 2, or 3.")
