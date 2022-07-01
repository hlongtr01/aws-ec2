from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.conf import SparkConf


spark = (SparkSession
    .builder
    .master("spark://localhost:7077")   
    .appName("read-json")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")   

conf = SparkConf() \
        .setAppName("read-json") \
        .set("spark.shuffle.service.enabled", "false") \
        .set("spark.dynamicAllocation.enabled", "false")


## Reading a JSON file into a DataFrame
json = 'api.json'
df = spark.read.format('json').load(json)


df2 = df.select(
    F.expr('rawData')
)
df2.printSchema()
df2.selectExpr("inline(rawData)").show(truncate=False)
