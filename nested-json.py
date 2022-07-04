from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.conf import SparkConf


## SPARK CONFIGURATION AND SPARK SESSION
spark_conf = SparkConf() \
        .setAppName("read-json") \
        .setMaster("spark://localhost:7077") \
        .set("spark.blockManager.port", "10025") \
        .set("spark.driver.blockManager.port", "10026") \
        .set("spark.driver.port", "10027") \
        .set("spark.cores.max", "1") \
        .set("spark.executor.memory", "1g") \
        .set("spark.driver.host", "localhost")

spark = (SparkSession
    .builder
    .config(conf=spark_conf) 
    .appName("read-json")
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
