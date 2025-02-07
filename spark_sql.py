from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import pyspark.pandas as ps
import numpy as np

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

# Apply function using sql()
df.createOrReplaceTempView("TAB")
spark.sql("select Seqno, Name, UPPER(Name) from TAB") \
     .show()

# Create custom function
def upperCase(str):
    return str.upper()

# Convert function to udf
upperCaseUDF = udf(lambda x:upperCase(x),StringType())

# Custom UDF with withColumn()
df.withColumn("Cureated Name", upperCaseUDF(col("Name"))) \
  .show(truncate=False)


# Custom UDF with select()
df.select(col("Seqno"), \
    upperCaseUDF(col("Name")).alias("Name") ) \
   .show(truncate=False)

# Custom UDF with sql()
spark.udf.register("upperCaseUDF", upperCaseUDF)
df.createOrReplaceTempView("TAB")
spark.sql("select Seqno, Name, upperCaseUDF(Name) from TAB") \
     .show()