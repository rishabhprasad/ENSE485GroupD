from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

## Update Spark Master with Container IP
SPARK_MASTER = "spark://172.20.0.3:7077"
KAFKA_BROKER = "broker:9092"
KAFKA_TOPIC = "testTopic2"

def main():
    sc = SparkContext(SPARK_MASTER)
    spark = SparkSession(sc)

    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

if __name__ == '__main__':
    main()