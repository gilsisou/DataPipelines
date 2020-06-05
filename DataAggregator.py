from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import count, desc


config = ConfigParser()
config.read('configuration.ini')

kafka_host = config["kafka"]["kafka-host"]
kafka_port = config["kafka"]["kafka-port"]
after_data_cleansing_kafka = config['kafka']['after_data_cleansing_kafka']

spark_jars_location = config['spark']['jars_location']
spark_shuffle_partitions = config['spark']['shuffle_partitions']

spark = SparkSession.builder.appName("DataAggregator") \
    .config("spark.jars", spark_jars_location + "spark-sql-kafka-0-10_2.11-2.4.5.jar") \
    .config("spark.sql.shuffle.partitions", spark_shuffle_partitions) \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f'{kafka_host}:{kafka_port}') \
    .option("subscribe", after_data_cleansing_kafka) \
    .load()\
    .selectExpr("CAST(value AS STRING)")

wordCounts = df\
    .groupBy("value")\
    .agg(count('*')
         .alias("word_count"))\
    .orderBy(desc("word_count"))\
    .limit(10)

query = wordCounts.writeStream \
    .option("checkpointLocation", 'C:\\anothertmp') \
    .format("console")\
    .outputMode("complete")\
    .start()

query.awaitTermination()
