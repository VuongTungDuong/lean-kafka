import time

from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "0.0.0.0:9092",
    "group.id": "my-group",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)
consumer.subscribe(["first_topic"])

print("📥 Waiting for messages...\n")

try:
    while True:
        msg = consumer.poll(5)
        if msg is None:
            print("⏳ No message received, waiting...")
            continue
        if msg.error():
            print(f"⚠️ Consumer error: {msg.error()}")
            continue

        print(
            f"✅ Received: key={msg.key().decode() if msg.key() else 'None'}, value={msg.value().decode()}"
        )
        time.sleep(0.1)
except KeyboardInterrupt:
    print("⛔ Interrupted by user")
finally:
    consumer.close()
