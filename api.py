import requests
from pyspark.sql import SparkSession
import json
from pyspark.conf import SparkConf


spark_conf = SparkConf() \
        .setAppName("read-json") \
        .setMaster("spark://localhost:7077") \
        .set("spark.shuffle.service.enabled", "false") \
        .set("spark.dynamicAllocation.enabled", "false") \
        .set("spark.executor.memory", "8G") \
        .set("spark.executor.cores", 2)
        

spark = (SparkSession
    .builder
    .config(conf=spark_conf)
    .getOrCreate())
sc=spark.sparkContext
sc.setLogLevel("WARN")


url = 'https://coronavirus.m.pipedream.net/'
response = requests.get(url)
data = response.content.decode('utf-8') ## Decode if needed, optional sometimes
json_data = json.loads(json.dumps(data)) ## Decode if needed, optional sometimes


json_rdd = sc.parallelize([json_data])
df = spark.read.json(json_rdd)


rawData_df = df.selectExpr('inline(rawData)')
rawData_trans_df = rawData_df.select('Province_State', 'Country_Region', 'Combined_Key', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Last_Update')
rawData_trans_df.show(truncate=False)
