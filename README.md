# ENSE 485 - Group D

## Members

Rishabh Prasad, William Peers, Nolan Flegel, Timothy Pasion, Joseph Bello

# Python Setup
pip install -r requirements.txt

# Kafka Sink Commands

```curl -X PUT http://localhost:8083/connectors/sink-postgres-test/config \
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
        "table.name.format":"AirQuality",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schemas.enable":"true",
        "value.converter.schema.registry.url": "http://schema-registry:8081",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "key.converter.schema.registry.url": "http://schema-registry:8081"
    }'
```

# Docker Commands
 - docker exec -it postgres bash
 - docker exec -it spark bash
 - docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' spark


# Spark Commands
## update spark IP address with spark docker container ip and update the spark.py file
- spark-client
- spark-submit --master spark://172.20.0.3:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 spark_stream.py

