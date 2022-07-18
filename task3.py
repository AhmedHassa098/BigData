
# We use a dictionary to store the city in broadcast then we use a data a use the keywords ,it find and takes then matches variable stored in broadcast
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

Cities = {"Lhr": "Lahore", "Isl": "Islamabad", "Kr": "Karachi"}
Citybroadcast = spark.sparkContext.broadcast(Cities)

data = [("Ahmed", "Hassan", "PAK", "Lhr"),
        ("Hamza", "Jutt", "PAK", "Kr"),
        ("Ali", "Malik", "PAK", "Isl"),
        ("Umer", "Akmal", "PAK", "Lhr")
        ]

rdd = spark.sparkContext.parallelize(data)


def City_convert(code):
    return Citybroadcast.value[code]


result = rdd.map(lambda x: (x[0], x[1], x[2], City_convert(x[3]))).collect()
print(result)


# COMMAND ----------

accumulatorBigData = sc.accumulator(0)
rdd = spark.sparkContext.parallelize([7, 2, 4, 4, 9])
rdd.foreach(lambda x: accumulatorBigData.add(x))
print(accumulatorBigData.value)
#Value is Access by driver and saved

# COMMAND ----------

LambaFunction = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
print(LambaFunction.reduce(lambda x, y: x + y))
#Add All
