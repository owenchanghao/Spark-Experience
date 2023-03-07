### DESCRIPTION:
'''
The intent of the code is to get all numbers below 50 from A and put them into B, and then get all numbers above 10 from B and put them into C.
Add one line of code so that it produces the desired behavior. You are not allowed to change the existing code.
'''

from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

A = sc.parallelize(range(1, 100))
t = 50
B = A.filter(lambda x: x < t)
B.persist()   # materialize B
count_B = B.count()
t = 10
C = B.filter(lambda x: x > t)
count_C = C.count()

result = [count_B, count_C]
print(result)
