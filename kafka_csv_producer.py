import csv, json, time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('data/reddit_database.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        text = row.get("post", "") or row.get("text", "")
        if text.strip():
            producer.send("reddit_csv_topic", value={"text": text})
            print("Sent:", text[:60])
            time.sleep(0.5)

producer.close()
