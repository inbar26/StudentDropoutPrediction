import sys
import subprocess
from Views.dashboard import Dashboard
from PyQt6.QtWidgets import QApplication
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import time


def reset_kafka_topics():
    admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

    topics = [
        "external_dataset",
        "manual_dataset_with_target",
        "manual_dataset_without_target",
        "predicted_manual_dataset"
    ]

    print("🧹 Deleting topics...")
    try:
        fs = admin.delete_topics(topics, operation_timeout=30)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"✅ Topic '{topic}' deleted.")
            except KafkaException as e:
                print(f"⚠️ Failed to delete topic {topic}: {e}")
    except Exception as e:
        print(f"⚠️ Error during topic deletion: {e}")

    # Wait until topics are fully removed
    print("⏳ Waiting for deletion to complete...")
    retries = 30
    while retries > 0:
        metadata = admin.list_topics(timeout=10)
        existing = set(metadata.topics.keys())
        pending = [t for t in topics if t in existing]
        if not pending:
            break
        print(f"⌛ Still deleting: {pending}")
        time.sleep(1)
        retries -= 1

    if retries == 0:
        print("⚠️ Timeout while waiting for topics to be deleted. Proceeding...")

    # Try to create missing topics
    for attempt in range(5):
        print(f"📦 Creating topics (attempt {attempt + 1})...")
        metadata = admin.list_topics(timeout=10)
        existing = set(metadata.topics.keys())
        to_create = [t for t in topics if t not in existing]

        if not to_create:
            print("✅ All topics created successfully.")
            break

        new_topics = [NewTopic(topic=t, num_partitions=1, replication_factor=1) for t in to_create]
        fs = admin.create_topics(new_topics)

        for topic, f in fs.items():
            try:
                f.result()
                print(f"✅ Topic '{topic}' created.")
            except KafkaException as e:
                print(f"⚠️ Failed to create topic {topic}: {e}")
        
        time.sleep(2)

    else:
        print("❌ Some topics still missing after retries. Check Kafka state manually.")


def run_producer():
    print("🚀 Running producer1.py to send static dataset to Kafka...")
    subprocess.run(["python3", "Models/producer1.py"])

def run_model():
    print("🧠 Training the model using model.py...")
    from Models.model import train_initial_model  # Lazy import to avoid early execution
    train_initial_model()

def run_dashboard():
    app = QApplication(sys.argv)
    dashboard = Dashboard()
    dashboard.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    reset_kafka_topics()
    run_producer()
    run_model()
    run_dashboard()
