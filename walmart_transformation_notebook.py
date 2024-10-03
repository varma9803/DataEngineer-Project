# Databricks notebook source
spark


# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("s3_raw_data").getOrCreate()

# COMMAND ----------

access_key = 'AKI'
secrect_key = 'Mzlo'
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secrect_key)
aws_region="us-east-2"
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3." + aws_region+ ".amazonaws.com" )
print("connection successful")

# COMMAND ----------

raw_data=spark.read.format("csv").option("header","true").option("inferschema","true").load("s3://raw-bucket-walmart/raw_data/rawdata_mysql.csv")
raw_data.show()

# COMMAND ----------

raw_data.printSchema()


# COMMAND ----------

raw_data=raw_data.drop('MarkDown1','MarkDown2','MarkDown3','MarkDown4','MarkDown5')
raw_data.display()

# COMMAND ----------

raw_data.filter((raw_data.IsHoliday == True) & (raw_data.Unemployment > 7)).show(5,False)

# COMMAND ----------

row_count = raw_data.count()
print(f"Number of rows: {row_count}")
raw_data.show(row_count, truncate=False)


# COMMAND ----------


distinct_store_data = raw_data.dropDuplicates(["Store"])
row_count1 = distinct_store_data.count()
print(f"Number of rows: {row_count1}")
distinct_store_data.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import when, col
#distinct_store_data.createOrReplaceTempView("result1")

result1=distinct_store_data.withColumn(
    "Temp_Level", 
    when(col("Temperature")>72, "High")
    .when(col("Temperature")<30, "Low")
    .otherwise("Medium")
    )
result1.show(truncate=False)

# COMMAND ----------

Output = result1.filter(col("Temp_Level") == "Low") \
                .select("Store", "Date", "Temperature", "Fuel_Price") \
                .orderBy("Store", ascending=True)
Output.show(truncate=False)

# COMMAND ----------

Output.write.mode("overwrite").option("header", "true").csv("s3://storgae-bucket-walmart/Transformed_data/Transformed_table.csv")

# COMMAND ----------


