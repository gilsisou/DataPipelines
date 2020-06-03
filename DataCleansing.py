from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import re

config = ConfigParser()
config.read('configuration.ini')
kafka_host = config["kafka"]["kafka-host"]
kafka_port = config["kafka"]["kafka-port"]
twitter_to_kafka_topic = config['kafka']['twitter_to_kafka_topic']
after_data_cleansing_kafka = config['kafka']['after_data_cleansing_kafka']

black_list_contains_words = ['#', '@', 'http://', 'https://']

spark = SparkSession.builder.appName("DataCleansing") \
    .config("spark.jars",
            "C:\Spark\spark-2.4.5-bin-hadoop2.7\jars\spark-streaming-kafka-0-10_2.11-2.1.0.jar,"
            "C:\Spark\spark-2.4.5-bin-hadoop2.7\jars\spark-sql-kafka-0-10_2.11-2.1.0.jar") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f'{kafka_host}:{kafka_port}') \
    .option("subscribe", twitter_to_kafka_topic) \
    .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df.select(explode(split(df.value, " ")).alias("word"))

ds = df.map(_.toString.getBytes).toDF("value")\
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", f'{kafka_host}:{kafka_port}') \
  .option("topic", "after_data_cleansing_kafka") \
  .start()




def check_if_message_starts_with_bad_char(input_word):
    for char in black_list_contains_words:
        if char in input_word:
            return False
    return True


def clean_word_from_nonalpabetic_chars(word):
    return re.sub(r'[^a-zA-Z]+', '', word)
#
#
# a = df.flatMap(lambda s: s.split(" ")) \
#     .filter(check_if_message_starts_with_bad_char) \
#     .map(clean_word_from_nonalpabetic_chars) \
#     .collect()

