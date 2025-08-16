import csv, json, time
from kafka import KafkaProducer
import praw

with open('client_id', 'r') as f:
    CLIENT_ID = f.read().strip()
    
with open('client_secret', 'r') as f:
    CLIENT_SECRET = f.read().strip()

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Reddit API setup
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent='SentimentAppByRaj'
)

subreddit = reddit.subreddit('all')  # or any subreddit you choose

for comment in subreddit.stream.comments(skip_existing=True):
    text = comment.body
    producer.send("reddit_csv_topic", value={"text": text})
    print("Sent:", text[:60])
    time.sleep(0.5)

producer.close()
