import time

from confluent_kafka.admin import AdminClient, NewPartitions

# Cấu hình Kafka
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "first_topic"
THRESHOLD_MESSAGES = 1000  # Ngưỡng giả lập
MAX_PARTITIONS = 6  # Số partition tối đa bạn cho phép
CHECK_INTERVAL = 30  # Thời gian chờ giữa các lần kiểm tra

# Khởi tạo AdminClient
admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def get_partition_count(topic_name):
    """Lấy số lượng partition hiện tại của topic"""
    metadata = admin_client.list_topics(timeout=10)
    topic = metadata.topics.get(topic_name)
    if topic is None:
        raise Exception(f"Topic '{topic_name}' không tồn tại.")
    return len(topic.partitions)


def scale_partitions_if_needed():
    """Giả lập kiểm tra tải và mở rộng partition nếu cần"""
    current_count = get_partition_count(TOPIC_NAME)
    print(f"📊 Topic '{TOPIC_NAME}' hiện có {current_count} partitions.")

    # ⚠️ Thay thế bằng dữ liệu thật từ metrics hệ thống
    fake_message_count = 1500

    if fake_message_count > THRESHOLD_MESSAGES and current_count < MAX_PARTITIONS:
        new_count = current_count + 1
        print(f"⚙️ Mở rộng partition: {current_count} → {new_count}")
        fs = admin_client.create_partitions([NewPartitions(TOPIC_NAME, new_count)])

        # Kiểm tra kết quả
        for topic, f in fs.items():
            try:
                f.result()  # sẽ raise lỗi nếu có
                print(f"✅ Đã mở rộng topic '{topic}' thành {new_count} partitions.")
            except Exception as e:
                print(f"❌ Không thể mở rộng topic '{topic}': {e}")
    else:
        print("✅ Không cần mở rộng partitions.")


# Lặp kiểm tra giả lập
if __name__ == "__main__":
    while True:
        try:
            scale_partitions_if_needed()
            time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            print("⛔ Dừng bởi người dùng.")
            break
        except Exception as e:
            print(f"❌ Lỗi: {e}")
            break
