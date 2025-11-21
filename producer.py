import time
import random
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Configuration
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

with open("order.avsc", "r") as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': lambda k, ctx: str(k).encode('utf-8'), # Simple string key
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)
TOPIC = 'orders'

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}]")

# Generate Data Loop
try:
    order_counter = 1000
    while True:
        order_counter += 1
        # Randomized data as per assignment requirement 
        order_data = {
            "orderId": str(order_counter),
            "product": random.choice(["Laptop", "Mouse", "Keyboard", "Monitor"]),
            "price": round(random.uniform(10.0, 500.0), 2)
        }

        producer.produce(topic=TOPIC, key=str(order_counter), value=order_data, on_delivery=delivery_report)
        producer.poll(0)
        time.sleep(1) # Slow down to see the logs clearly

except KeyboardInterrupt:
    pass
finally:
    producer.flush()