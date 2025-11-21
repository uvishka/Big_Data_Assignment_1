from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

# --- CONFIGURATION ---
SR_URL = 'http://localhost:8081'
BOOTSTRAP = 'localhost:9092'
MAIN_TOPIC = 'orders'
DLQ_TOPIC = 'orders_dlq'

# Load Schema
with open("order.avsc", "r") as f:
    schema_str = f.read()

# Schema Registry Clients
sr_client = SchemaRegistryClient({'url': SR_URL})
avro_deserializer = AvroDeserializer(sr_client, schema_str)
avro_serializer = AvroSerializer(sr_client, schema_str)

# Consumer Config
consumer_conf = {
    'bootstrap.servers': BOOTSTRAP,
    'group.id': 'order_group',
    'auto.offset.reset': 'earliest',
    'key.deserializer': lambda k, ctx: k, 
    'value.deserializer': avro_deserializer
}

# Producer Config (For sending to DLQ)
dlq_producer_conf = {
    'bootstrap.servers': BOOTSTRAP,
    'key.serializer': lambda k, ctx: str(k).encode('utf-8'),
    'value.serializer': avro_serializer
}

consumer = DeserializingConsumer(consumer_conf)
dlq_producer = SerializingProducer(dlq_producer_conf)
consumer.subscribe([MAIN_TOPIC])

# --- AGGREGATION STATE ---
total_price_sum = 0.0
total_count = 0

def process_order(order):
    """
    Simulate processing. Raises an error for specific conditions 
    to demonstrate Retry and DLQ logic.
    """
    # SIMULATED FAILURE: Fail if price is > 450 to test DLQ
    if order['price'] > 450:
        raise ValueError("Price too high for automated processing!")
    
    return True

def send_to_dlq(key, value, error_msg):
    """Send permanently failed messages to DLQ """
    print(f"!!! Sending Order {value['orderId']} to DLQ. Reason: {error_msg}")
    dlq_producer.produce(topic=DLQ_TOPIC, key=key, value=value)
    dlq_producer.flush()

# --- MAIN LOOP ---
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        order = msg.value()
        key = msg.key()

        # RETRY LOGIC 
        max_retries = 3
        attempt = 0
        success = False
        
        while attempt < max_retries:
            try:
                process_order(order)
                
                # If successful, update Real-time Aggregation 
                total_price_sum += order['price']
                total_count += 1
                running_avg = total_price_sum / total_count
                
                print(f"Processed Order {order['orderId']} | Price: ${order['price']} | Running Avg: ${running_avg:.2f}")
                success = True
                break # Exit retry loop

            except Exception as e:
                attempt += 1
                print(f"swarning: Processing failed for Order {order['orderId']} (Attempt {attempt}/{max_retries})")

        # DEAD LETTER QUEUE LOGIC 
        if not success:
            send_to_dlq(key, order, "Max retries exceeded")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()