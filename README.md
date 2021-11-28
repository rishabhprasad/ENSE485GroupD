# ENSE 485 - Group D

## Members

- Rishabh Prasad
- William Peers
- Nolan Flegel
- Timothy Pasion
- Joseph Bello

## Project Description

The goal of our project is to create predictions of the air quality of many cities
using historical data.

### Dataset

Our project will accomplish our goal through the use of a dataset named the Worldwide COVID-19 dataset provided by the Air Quality Open Data Platform. The goal of this project is to create predictions for air quality based upon previous data using the MLlib and Graphx libraries from Apache Spark.

### Website

To accompany our project we have created a website which allows a user to select a city and see predictions for the selected cities air quality. For instructions to run
this site locally, see the section at the bottom of this readme.

### Dependencies

- MLlib
- GraphX
- Flask
- Pyscopg2

## Python Setup

pip install -r requirements.txt

## Commands

### Kafka Sink Commands

```bash

curl -X PUT http://localhost:8083/connectors/sink-postgres-airQ/config \
    -H "Content-Type: application/json" \
    -d '{
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "connection.url": "jdbc:postgresql://postgres:5432/ENSE485",
        "connection.user": "aqicn",
        "connection.password": "GuiltySpark343",
        "tasks.max": "1",
        "topics":"AirQuality2020",
        "db.name":"ENSE485",
        "auto.create":"true",
        "auto.evolve":"true",
        "pk.mode":"kafka",
        "insert.mode": "upsert",
        "db.timezone":"UTC",
        "table.name.format":"airquality",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schemas.enable":"true",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "key.converter.schema.registry.url": "http://schema-registry:8081"
    }'
```

### Docker Commands

- docker exec -it postgres bash
- docker exec -it spark bash
- docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark

### Spark Commands

#### update spark IP address with spark docker container ip and update the spark.py file

- spark-client
- spark-submit --master spark://172.20.0.3:7077 --packages org.apache.
- spark:spark-sql-kafka-0-10_2.12:3.2.0 spark_stream.py

## Running the website locally

Delete your cloned "venv" folder. Run this command to create a new virtual environment. A new folder will be created.

```bash
python3 -m venv venv
```

Execute this command to activate the environment.

```bash
source venv/bin/activate
```

Make sure Flask is installed

```bash
pip3 install Flask
```

Set the FLASK_APP system variable

```bash
export FLASK_APP=application.py
```

Install psycopg2

```bash
pip install psycopg2
```

Run Flask.

```bash
flask run
```
