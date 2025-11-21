# Kafka Order Processing System

This project implements a real-time order processing system using Apache Kafka and Python. It is designed to fulfill the requirements by demonstrating Avro serialization, real-time data aggregation, and error handling patterns.

## Project Overview
The system consists of a **Producer** that generates mock order data and a **Consumer** that processes these orders. It handles:
* **Avro Serialization:** Enforces strict schema validation using the Confluent Schema Registry.
* **Real-time Aggregation:** Calculates and displays a running average of order prices.
* **Fault Tolerance:** Implements automatic retry logic for processing failures.
* **Dead Letter Queue (DLQ):** Permanently failed messages are routed to a separate `orders_dlq` topic for isolation.

## File Structure
* `docker-compose.yml`: Infrastructure setup (Kafka, Zookeeper, Schema Registry).
* `order.avsc`: The Avro schema definition for Order messages.
* `producer.py`: Script to generate and serialize random orders.
* `consumer.py`: Script to consume, deserialize, aggregate, and handle errors.
* `requirements.txt`: Python dependencies.

## Prerequisites
* **Docker Desktop** (Must be installed and running).
* **Python 3.x**

## Setup Instructions

## 1. Start Infrastructure
Run the following command to start Kafka and the Schema Registry:
```bash
docker-compose up -d
```
## 2. Install Dependencies

## Install the required Python libraries:

```bash
pip install -r requirements.txt
```

## 3. How to Run

To run the system, you will need two separate terminal windows open in VS Code.

## Terminal 1: Start the Consumer

The consumer needs to be running first (or simultaneously) to process messages.

```bash
python consumer.py
```

You will see the consumer start up and wait for messages.

## Terminal 2: Start the Producer

In the second terminal, run the producer to start generating data.

```bash
python producer.py
```
You will see logs indicating that orders are being produced.