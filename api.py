import requests
import json
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


spark_conf = SparkConf() \
        .setAppName("api") \
        .setMaster("spark://192.168.245.130:7077") \
        .set("spark.executor.memory", "8GB") \
        .set("spark.executor.cores", 1) \
        .set("spark.shuffle.service.enabled", "false") \
        .set("spark.dynamicAllocation.enabled", "false")   
##        .set("spark.num.executors", 1) \
##        .set("spark.driver.memory", "1G") \
     

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
