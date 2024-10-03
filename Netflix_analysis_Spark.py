# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import SparkSession
spark= SparkSession.builder.appName("Netflix data").getOrCreate()

# COMMAND ----------

access_key = 'AKI'
secrect_key = 'Mzlo'
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secrect_key)
aws_region="us-east-2"
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3." + aws_region+ ".amazonaws.com" )



# COMMAND ----------

credits= spark.read.format("csv").option("header","true").option("inferschema","true").load("s3://storagelayer/netflix dataset/credits.csv")
credits.display()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, BooleanType, DecimalType 

# COMMAND ----------

titles_schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("age_certification", StringType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("genres", StringType(), True),
    StructField("production_countries", StringType(), True),
    StructField("seasons", IntegerType(), True),
    StructField("imdb_id", StringType(), True),
    StructField("imdb_score", DecimalType(10, 2), True),  # Specify precision and scale for DecimalType
    StructField("imdb_votes", StringType(), True),
    StructField("tmdb_popularity", DecimalType(10, 2), True),  # Specify precision and scale for DecimalType
    StructField("tmdb_score", DecimalType(10, 2), True)  # Specify precision and scale for DecimalType
])

# COMMAND ----------

title=spark.read.schema(titles_schema).format("csv").option("header","true").load("s3://storagelayer/netflix dataset/titles.csv")

# COMMAND ----------

title.show()

# COMMAND ----------

credits.createOrReplaceTempView("credits")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from credits where name="Robert De Niro";

# COMMAND ----------

result=spark.sql("select * from credits where character='Girl in Castle Anthrax'")
result.head(5)

# COMMAND ----------

title=title.na.fill({"age_certification":"Not Rated", "seasons": 0})
title.display()

# COMMAND ----------

from pyspark.sql.functions import col, when

title = title.withColumn(
    "Hit Level",
    when(
        ((col("imdb_score") > 8) & (col("tmdb_score") > 8)) | (col("imdb_votes") > 40),
        True 
    ).otherwise(False)
)

# COMMAND ----------

title=title.withColumn( "Film Type",
                       when(
                           (col("release_year")<2000) & (col("tmdb_popularity")>30), "Classics")
                       .when(
                           (col("release_year")>2000) & (col("tmdb_popularity")>30), "New Hits")
                       .otherwise("Non-Hits")
                       )

# COMMAND ----------

title.display()

# COMMAND ----------

#join= credits.join(title, credits.id==title.id, "inner")
credits = credits.withColumnRenamed("id", "credits_id")
title = title.withColumnRenamed("id", "title_id")
title = title.withColumnRenamed("Film Type", "Film_Type")

join = credits.join(title, credits.credits_id == title.title_id, "inner")
join.display()


# COMMAND ----------


join.createOrReplaceTempView("joined_table")
result=spark.sql("SELECT credits_id, title, name, Film_Type FROM joined_table WHERE type='MOVIE' AND imdb_score > 8")
result.show(40)

# COMMAND ----------

max_rating=spark.sql("SELECT DISTINCT release_year, title, imdb_score FROM joined_table WHERE (release_year, imdb_score) IN (SELECT release_year, MAX(imdb_score) FROM joined_table GROUP BY release_year) Order by release_year ASC")
max_rating.show()

# COMMAND ----------

from matplotlib import pyplot as plt
import seaborn as sns
vis=max_rating.toPandas()
plt.figure(figsize=(12,6))
sns.barplot(x='title', y='imdb_score', data=vis)
plt.title=('Highest Rating Per Year')
plt.xlabel('Title')
plt.ylabel('Rating')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

# COMMAND ----------

max_rating.write.mode("overwrite").saveAsTable("max_rating_table");

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

max_rating.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://storagelayer/databricks op file/")

# COMMAND ----------


