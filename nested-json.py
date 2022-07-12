from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.conf import SparkConf



## SPARK CONFIGURATION AND SPARKSESSION INITIATE
spark_conf = SparkConf() \
        .setAppName("read-json") \
        .set("spark.executor.memory", "1g")             
##        .setMaster("spark://localhost:7077") \
##        .set("spark.executor.cores", 1) \
##        .set("spark.driver.host", "localhost")

spark = (SparkSession
    .builder
    .config(conf=spark_conf) 
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")   




## Reading a JSON file into a DataFrame
json = 'api.json'
df = spark.read.format('json').load(json)


df2 = df.select(
    F.expr('rawData')
)
df2.printSchema()
df2.selectExpr("inline(rawData)").show(truncate=False)
