from confluent_kafka import Producer

conf = {"bootstrap.servers": "0.0.0.0:9092"}
producer = Producer(conf)
print(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")


# Gửi 10 message vào topic
for i in range(10):
    producer.produce(
        "first_topic", key=str(i), value=f"Message {i}", callback=delivery_report
    )
    producer.poll(0)

producer.flush()
print("All messages sent.")
