from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StringType, StructType
import re, nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

nltk.download('stopwords')
nltk.download('wordnet')

with open('access_key.txt', 'r') as f:
    ACCESS_KEY = f.read().strip()

with open('secret_key.txt', 'r') as f:
    SECRET_KEY = f.read().strip()

# Preprocess
stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

def clean_text(text):
    text = re.sub(r"http\S+|www\S+|https\S+", '', text)
    text = re.sub(r'[^A-Za-z\s]', '', text.lower())
    tokens = text.split()
    return " ".join(lemmatizer.lemmatize(w) for w in tokens if w not in stop_words)

def get_sentiment(text):
    if not text:
        return "neutral"
    pos = {'good', 'great', 'excellent', 'love', 'happy'}
    neg = {'bad', 'terrible', 'awful', 'hate', 'sad'}
    words = text.split()
    score = sum((w in pos) - (w in neg) for w in words)
    return 'positive' if score > 0 else 'negative' if score < 0 else 'neutral'

spark = SparkSession.builder \
    .appName("RedditSentimentStream") \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

schema = StructType().add("text", StringType())
clean_udf = udf(clean_text, StringType())
sentiment_udf = udf(get_sentiment, StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit_csv_topic") \
    .option("startingOffsets", "latest") \
    .load().selectExpr("CAST(value AS STRING)")

json_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")
processed = json_df.withColumn("clean_text", clean_udf(col("text")))
scored = processed.withColumn("sentiment", sentiment_udf(col("clean_text")))

# scored.select("text", "clean_text", "sentiment").show(truncate=False)


# query = scored.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "s3a://reddit-streaming-data/reddit_output") \
#     .option("checkpointLocation", "s3a://reddit-streaming-data/reddit_ckpt") \
#     .start()

# query.awaitTermination()
# json_df.writeStream \
#     .format("console") \
#     .start() \
#     .awaitTermination()
# query = scored.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()
# scored.select("text", "clean_text", "sentiment") \
#     .writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "s3a://reddit-streaming-data/reddit_output") \
#     .option("checkpointLocation", "s3a://reddit-streaming-data/reddit_ckpt") \
#     .start() \
#     .awaitTermination()

query = scored.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3a://reddit-streaming-data/reddit_output") \
    .option("checkpointLocation", "s3a://reddit-streaming-data/reddit_ckpt") \
    .start()

query.awaitTermination()
