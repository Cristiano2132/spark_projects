from pyspark.sql import SparkSession
from pyspark.sql.functions import upper

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)

# Apply function using withColumn

df.withColumn("Upper_Name", upper(df.Name)) \
  .show()

# Apply function using select
df.select("Seqno","Name", upper(df.Name).alias("Upper_Name")) \
    .show()