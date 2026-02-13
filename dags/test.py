from pyspark.sql import SparkSession
import os

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

spark = SparkSession.builder \
    .appName("TestSpark") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.python.worker.timeout", "300") \
    .getOrCreate()

data = [("Satish", 100), ("Kumar", 200)]
columns = ["Name", "Value"]

df = spark.createDataFrame(data, columns)
df.show()

spark.stop()
