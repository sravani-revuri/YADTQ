# YADTQ: Yet Another Distributed Task Queue

**YADTQ** is a distributed task queue system using **Kafka** and **Redis** to manage and process tasks efficiently.

## **Features**
YADTQ enables task submission, processing, and tracking with periodic worker heartbea

## **Components**
### **1. Client (`client.py`)**
- Submits tasks to Kafka.  
- Checks task status from Redis.  
- Handles graceful shutdown.

### **2. Worker (`worker.py`)**
- Listens for tasks from Kafka.  
- Processes tasks and updates Redis.  
- Sends periodic heartbeats for monitoring.

### **3. Task Queue (`yadtq.py`)**
- Manages task submission to Kafka.  
- Maintains task state in Redis.  
- Logs task status and worker heartbeats.

## **Prerequisites**
- Python 3.x  
- Kafka & Zookeeper  
- Redis

## **Usage**

### **1. Start Kafka and Redis**
Ensure Kafka and Redis servers are running.

### **2. Create Topics**
kafka-topics.sh --create --topic task_queue --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

kafka-topics.sh --create --topic heartbeat_queue --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

### **2. Start 3 Workers on separate terminals**
```bash
python3 worker.py --worker-id worker_1

python3 worker.py --worker-id worker_2

python3 worker.py --worker-id worker_3
```
### **3. Starta client**
```bash
python3 client.py
```


Continue to choose the options of submitting a task or checking status as you wish!

The heartbeats.log file periodically updates worker hearbeats and logs error is worker is unresponsive and task re-assignment messages.

The worker.log file logs task details


