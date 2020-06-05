from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import lit, col, udf, explode, split
from pyspark.sql.types import BooleanType, StringType
import re

config = ConfigParser()
config.read('configuration.ini')
kafka_host = config["kafka"]["kafka-host"]
kafka_port = config["kafka"]["kafka-port"]
twitter_to_kafka_topic = config['kafka']['twitter_to_kafka_topic']
after_data_cleansing_kafka = config['kafka']['after_data_cleansing_kafka']

spark_jars_location = config['spark']['jars_location']

black_list_startwith_words = ['#', '@', '&', 'http://', 'https://']

non_relevant_words = ['rt', 'the']
# rt is not relevant because each re-twit start with this word.
# you can add any word you would not like to count !!


def check_if_word_starts_with_bad_char(input_word):
    print(input_word)
    for char in black_list_startwith_words:
        if char in input_word:
            return False
    return True


def check_if_word_is_relevant(input_word):
    print(input_word)
    for word in non_relevant_words:
        if word == input_word:
            return False
    return True


def clean_word_from_nonalpabetic_chars(input_word):
    return str(re.sub(r'[^a-zA-Z]+', '', input_word)).lower()


clean_word = udf(clean_word_from_nonalpabetic_chars)
check_word_starts_with = udf(check_if_word_starts_with_bad_char, BooleanType())
check_word_relevance = udf(check_if_word_is_relevant, BooleanType())

spark = SparkSession.builder.appName("DataCleansing") \
    .config("spark.jars", spark_jars_location + "spark-sql-kafka-0-10_2.11-2.4.5.jar") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f'{kafka_host}:{kafka_port}') \
    .option("subscribe", twitter_to_kafka_topic) \
    .load()

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
df = df.select(explode(split(df.value, " ")).alias("word"))
df = df.where(check_word_starts_with("word"))
df = df.withColumn("word", clean_word("word"))
df = df.where(check_word_relevance("word"))
df = df.where(col("word") != '')  # for case that the function clean_word() deleted all the chars from the word

ds = df \
    .withColumnRenamed("word", "value") \
    .withColumn("key", lit(None).cast(StringType())) \
    .writeStream \
    .option("checkpointLocation", 'C:\\tmp') \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f'{kafka_host}:{kafka_port}') \
    .option("topic", after_data_cleansing_kafka) \
    .start()

ds.awaitTermination()
