from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.avro.functions import from_avro, to_avro

from confluent_kafka.schema_registry import SchemaRegistryClient


## Update Spark Master with Container IP
SPARK_MASTER = "spark://172.20.0.5:7077"
KAFKA_BROKER = "http://localhost:9092"
KAFKA_TOPIC = "AirQuality2020"
# SCHEMA_REGISTRY_URL = "http://172.20.0.8:8081"
schema_url = "http://localhost:8081"

def foreach_batch(df, epoch_id):
    jdbcProperties = {"user":"aqicn", "password":"GuiltySpark343",  "driver": 'org.postgresql.Driver'} # "driver": 'com.mysql.jdbc.Driver'
    df.write.jdbc(url="jdbc:postgresql://localhost:5432/ENSE485", table="unhealthy_pm25",
    mode="append", properties=jdbcProperties)

def main():
    
    spark = SparkSession.builder.appName("SSAirQuality").getOrCreate() 

    schema_registry_conf = {'url': schema_url}

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    aqicn_schema_response = schema_registry_client.get_latest_version(KAFKA_TOPIC+"-value").schema
    aqicn_schema = aqicn_schema_response.schema_str

    spark.sparkContext.setLogLevel('error')

    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC) 
        .option("startingOffsets", "earliest") 
        .load()
    )

    from_avro_options= {"mode":"PERMISSIVE"}

    ##Filter Categories
    # Unhealthy 60-120
    # Hazardous > 120

    hazardousDF = (
        df
        .select(from_avro(F.expr("substring(value, 6, length(value)-5)"), aqicn_schema, from_avro_options).alias("value"))
        .selectExpr("value.date", "value.country", 
                    "value.city", "value.specie", 
                    "value.count", "value.min","value.max",
                    "value.median","value.variance")
        .where("value.specie == 'pm25' AND (value.median > 60 AND value.median < 120)")
    )

    # hazardousDF.writeStream \
    # .format('console') \
    # .outputMode('update') \
    # .option("truncate", "false") \
    # .start()
    
    # outputDF = hazardousDF.selectExpr('to_json(struct(*)) as value') \
        # .select(to_avro("value", aqicn_schema).alias("value"))
    # outputDF = hazardousDF
    # outputDF.printSchema()

    outputDF = hazardousDF.selectExpr('to_json(struct(*)) as value') \
        # .select(F.to_json("value").alias("value"))

    outputDF.writeStream \
    .format('console') \
    .outputMode('update') \
    .option("truncate", "false") \
    .start()

    
    # outputDF.writeStream \
    # .format("kafka") \
    # .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    # .option("topic", "unhealthyPM25") \
    # .option("checkpointLocation","\tmp\kafka\checkpoint") \
    # .start() \
    # .awaitTermination()

    outputDF.writeStream \
    .foreachBatch(foreach_batch) \
    .start() \
    .awaitTermination()
    
if __name__ == '__main__':
    main()