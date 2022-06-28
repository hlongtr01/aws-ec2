import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json

spark = (SparkSession
    .builder
    .appName("api-data")
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
rawData_trans_df = rawData_df.withColumn('Id', monotonically_increasing_id()) \
    .select('Id', 'Province_State', 'Country_Region', 'Combined_Key', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Last_Update')
rawData_trans_df.show(truncate=False)

rawData_trans_df.write.format('jdbc') \
        .mode('overwrite') \
        .option("url", "jdbc:postgresql://192.168.245.128:5432/dev") \
        .option("dbtable", "covid_api") \
        .option('user', 'postgres') \
        .option('password', 'ImZero!0w0') \
        .save()
