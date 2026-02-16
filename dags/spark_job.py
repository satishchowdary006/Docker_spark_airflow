from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create Spark session
spark = SparkSession.builder \
    .appName("EmployeeSalaryApp") \
    .getOrCreate()

# Sample data
data = [
    ("Satish", "IT", 50000),
    ("Ravi", "IT", 60000),
    ("Anita", "HR", 45000),
    ("Kumar", "HR", 40000),
    ("Priya", "Finance", 70000)
]

columns = ["Name", "Department", "Salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

print("=== Employee Data ===")
df.show()

# Calculate average salary by department
avg_salary = df.groupBy("Department").agg(avg("Salary").alias("AverageSalary"))

print("=== Average Salary by Department ===")
avg_salary.show()

# Save output
avg_salary.write.mode("overwrite").csv("output")

spark.stop()
