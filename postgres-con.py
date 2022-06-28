from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://192.168.245.128:5432/dev") \
    .option("dbtable", "covid_api") \
    .option("user", "postgres") \
    .option("password", "ImZero!0w0") \
    .load()

df.printSchema()


                                                                                                                       