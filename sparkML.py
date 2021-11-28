
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import *
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import DenseVector

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/opt/zeppelin/local-repo/spark/postgresql-42.3.1.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/ENSE485") \
    .option("dbtable", "airquality") \
    .option("user", "aqicn") \
    .option("password", "GuiltySpark343") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# df.show()

dataset = df.select(     col('count').cast('float'),
                         col('min').cast('float'),
                         col('max').cast('float'),
                         col('median').cast('float'),
                         col('variance').cast('float'),
                         col('Specie'),
                         col('Country'),
                         col('City')
                        )
dataset = StringIndexer(
    inputCol='Country', 
    outputCol='iCountry',
    handleInvalid='skip').fit(dataset).transform(dataset)

dataset = StringIndexer(
    inputCol='City', 
    outputCol='iCity', 
    handleInvalid='skip').fit(dataset).transform(dataset)       

dataset = StringIndexer(
    inputCol='Specie', 
    outputCol='iSpecie', 
    handleInvalid='skip').fit(dataset).transform(dataset)       

# dataset = dataset.drop('Country')
# dataset = dataset.drop('City')
# dataset = dataset.drop('id')

required_features = ['iCity',
                    'iCountry',
                    'iSpecie',
                    'min',
                    'max',
                    'count',
                    'variance',
                  ]
                   
# from pyspark.ml.feature import VectorAssembler  as va
assembler = VectorAssembler(inputCols=required_features, outputCol='features')

transformed_data = assembler.setHandleInvalid("skip").transform(dataset)

(training_data, test_data) = transformed_data.randomSplit([0.8,0.2])

# from pyspark.ml.regression import LinearRegression
r = LinearRegression(labelCol='median', 
                            featuresCol='features')
                            
r = r.fit(training_data)

r.coefficients
r.intercept

predictions = r.evaluate(test_data)

output = predictions.predictions.select("Country","City","specie","prediction").where("specie = 'pm25'")

output.printSchema()

# output.write \
#     .csv("./data/predictions/")
output.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/ENSE485") \
    .option("dbtable", "aq_predictions") \
    .option("user", "aqicn") \
    .option("password", "GuiltySpark343") \
    .option("driver", "org.postgresql.Driver") \
    .save()

