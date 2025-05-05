# Setup Docker Kafka

## Kafka

Kafka và Zookeeper

```bash
docker compose -f docker-kafka-sever.yaml
```

## types-confluent-kafka hỗ trợ gợi ý kiểu dữ liệu của confluent-kafka

```python
pip install types-confluent-kafka
```

## confluent-kafka thư viện kết nối tới kafka

```python
pip install confluent-kafka
```

## Run test venv

Run producer

```python
python3 src/producer.py  
```

Run consumer

```python
python3 src/consumer.py
```
