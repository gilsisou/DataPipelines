from pyspark.sql import SparkSession
from configparser import ConfigParser
from kafka import KafkaConsumer
import re

config = ConfigParser()
config.read('configuration.ini')
kafka_host = config["kafka"]["kafka-host"]
kafka_port = config["kafka"]["kafka-port"]
twitter_to_kafka_topic = config['kafka']['twitter_to_kafka_topic']

consumer = KafkaConsumer(twitter_to_kafka_topic, bootstrap_servers=[f'{kafka_host}:{kafka_port}'])

spark = SparkSession.builder.appName("DataCleansing").getOrCreate()
sc = spark.sparkContext

black_list_contains_words = ['#', '@', 'http://', 'https://']


def cleansing_message(tweet):
    return tweet.flatMap(lambda message: message.split(" "))\
        .filter(lambda word: check_if_message_starts_with_bad_char(word))\
        .collect()


def check_if_message_starts_with_bad_char(input_word):
    for char in black_list_contains_words:
        if char in input_word:
            return False
    return True


for tweet in consumer:
    print(tweet.value.decode('ascii'))
    tweet_value = sc.parallelize([tweet.value.decode('ascii').lower()])
    filtered_tweet_rdd = tweet_value.flatMap(lambda s: s.split(" "))\
        .filter(lambda word: check_if_message_starts_with_bad_char(word)).collect()
    for word in filtered_tweet_rdd:
        cleaning_word = re.sub(r'[^a-zA-Z]+', '', word)
        print(cleaning_word)

