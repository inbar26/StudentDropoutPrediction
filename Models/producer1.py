from pyspark.sql import SparkSession
from confluent_kafka import Producer, Consumer
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStudentProducer") \
    .getOrCreate()

# Read the dataset using Spark (with partitions for better parallelism)
file_path = "/Users/rubinroy/Desktop/BigDataFinalProject/StudentDropoutPrediction/StudendsDataset.csv"
df = spark.read.option("header", "true").csv(file_path).repartition(4)  # Increase partitions for parallelism

total_rows = df.count()
print(f"📄 Total rows in CSV: {total_rows}")


rows = df.collect()

producer = Producer({'bootstrap.servers': 'localhost:9092'})

for row in rows:
    try:
        student_data = row.asDict()
        producer.produce('external_dataset', value=json.dumps(student_data).encode('utf-8'))
    except Exception as e:
        print(f"❌ Error sending row: {student_data}")
        print(f"    ↪️ Exception: {str(e)}")

producer.flush()


# Process the dataset in parallel using Spark
#df.rdd.foreachPartition(send_to_kafka)

topic = "external_dataset"
bootstrap_servers = 'localhost:9092'

consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'csv-checker',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
consumer.subscribe([topic])


count = 0
while True:
    msg = consumer.poll(5.0)
    if msg is None:
        break
    if msg.error():
        print(f"⚠️ Consumer error: {msg.error()}")
        continue

    data = json.loads(msg.value().decode('utf-8'))
    count += 1
    if count % 500 == 0:
        print(f"📦 Read {count} messages...")

print(f"✅ Total messages in Kafka topic '{topic}': {count}")
consumer.close()

print("✅ All data has been successfully sent in parallel!")
