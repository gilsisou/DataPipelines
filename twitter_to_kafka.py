import tweepy
from configparser import ConfigParser
from kafka import KafkaProducer
from time import sleep
from json import dumps


config = ConfigParser()
config.read('configuration.ini')

kafka_host = config["kafka"]["kafka-host"]
kafka_port = config["kafka"]["kafka-port"]
twitter_to_kafka_topic = config['kafka']['twitter_to_kafka_topic']

consumer_key = config["twitter"]["consumer_key"]
consumer_secret = config["twitter"]["consumer_secret"]
access_token = config["twitter"]["access_token"]
access_token_secret = config["twitter"]["access_token_secret"]

producer = KafkaProducer(bootstrap_servers=[f'{kafka_host}:{kafka_port}'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

for tweet in tweepy.Cursor(api.search, q="#food", lang="en").items():
    print("inserting to kafka:", tweet.text.lower())
    producer.send(topic=twitter_to_kafka_topic, value=tweet.text.lower())
    sleep(3)

producer.flush()
