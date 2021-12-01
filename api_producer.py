import json
import time
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

def msg_to_json(msg,city):
    
    jsonMsg ={
            "timestamp":str(msg["time"]['v']),
            "city": city,
            "pm25":str(msg["iaqi"]["pm25"]["v"]),
            "h":str(msg["iaqi"]["h"]["v"]),
            "t":str(msg["iaqi"]["t"]["v"]),
            "w":str(msg["iaqi"]["w"]["v"])
            }
    return jsonMsg

def call_producer(message, bootstrap_servers, schema_url, topic):
    schema_str = """
    {
        "namespace": "test.ense485",
        "name": "aqicn_api",
        "type": "record",
        "fields": [
            {"name": "timestamp", "type":"string"},
            {"name": "city", "type": "string"},
            {"name": "pm25", "type":"string"},
            {"name": "h", "type":"string"},
            {"name": "t", "type":"string"},
            {"name": "w", "type":"string"}
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

def fetch_api_response(api_key, city):
    base_url = "https://api.waqi.info"
    url = base_url + f'/feed/{city}/?token={api_key}'
    result = requests.get(url)
    return result

def main():
    options = ["Adana",
        "Akita",
        "Ankara",
        "Calama",
        "Canberra",
        "Colombo",
        "Darwin",
        "Fukuoka",
        "Hiroshima",
        "Istanbul",
        "Kagoshima",
        "Kaunas",
        "Kochi",
        "Konya",
        "Launceston",
        "Mashhad",
        "Matsuyama",
        "Melbourne",
        "Niigata",
        "Quito",
        "Rancagua",
        "Saitama",
        "Sapporo",
        "Sivas",
        "Stavanger",
        "Sydney",
        "Takamatsu",
        "Tehran",
        "Toyama",
        "Trabzon",
        "Trondheim",
        "Wakayama",
        "Wollongong"]

    bootstrap_servers= 'localhost:9092'
    schema_url = "http://localhost:8081"
    topic = 'aqicn_api'
    

    for city in options:
        r = fetch_api_response(config['token'], city)
        message = r.content
        message = json.loads(message)
        message = msg_to_json(message["data"], city)
        call_producer(message, bootstrap_servers, schema_url, topic)
        print(f'Message Sent: {message}')

if __name__ == '__main__':
    
    while True:
        main()
        time.sleep(30)