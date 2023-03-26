# Filter Brazil and select name & price information from data
# Source: http://www.cse.ust.hk/msbd5003/data/sales.csv

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import *

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.csv('./dataSet/sales.csv', header=True, inferSchema=True)

records = df.filter(df["Country"] == "Brazil")

result = records.rdd.map(lambda x: (x['Name'],x['Price'])).collect()

print(result)
