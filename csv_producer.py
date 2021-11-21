## references: https://github.com/billydh/python-avro-producer/
##              https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html

import json
from pandas.io.parsers import read_csv
import requests
from uuid import uuid4
import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from utilities.config import load_config
from utilities.load_avro_schema import load_avro_schema
config = load_config()

def msg_to_dict(msg, ctx):
    return dict (city=msg.city, lat=msg.lat, long=msg.long, pm25=msg.pm25)

def readCSV(filepath):
    df = pd.read_csv(filepath, dtype=str)
    return df

def call_producer(message, bootstrap_servers, schema_url, topic):
    schema_str = """
    {
        "namespace": "test.ense485",
        "name": "myTestRecord",
        "type": "record",
        "fields": [
            {"name": "date", "type":"string"},
            {"name": "country", "type":"string"},
            {"name": "city", "type": "string"},
            {"name": "specie", "type":"string"},
            {"name": "count", "type":"string"},
            {"name": "min", "type":"string"},
            {"name": "max", "type":"string"},
            {"name": "median", "type":"string"},
            {"name": "variance", "type":"string"}
        ]
    }
    """

    schema_registry_conf = {'url': schema_url}

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    avro_serializer = AvroSerializer(schema_registry_client,schema_str)
    producer_conf = {'bootstrap.servers': bootstrap_servers,
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}
    producer = SerializingProducer(producer_conf)
    producer.produce(topic=topic, key=str(uuid4()), value=message)
    producer.flush()

def main():
    bootstrap_servers= 'http://localhost:9092'
    schema_url = "http://localhost:8081"
    topic = 'AirQuality2020'
    filepath = './data/waqi-covid19-airqualitydata-2020.csv'
    df = readCSV(filepath)
    for index, row in df.iterrows():
        message={
            "date":row['Date'],
            "country":row['Country'],
            "city":row['City'],
            "specie":row['Specie'],
            "count":row['count'],
            "min":row['min'],
            "max":row['max'],
            "median":row['median'],
            "variance":row['variance']
            }
        call_producer(message, bootstrap_servers, schema_url, topic)
        print(f'Message Sent: {message}')
        
if __name__ == '__main__':
   main()