import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

db_username="root"
db_password="newpassword"
db_url="jdbc:mysql://0.tcp.ngrok.io:16415/walmart"
table_name="walmart.store_data"
jdbc_driver_name="com.mysql.cj.jdbc.Driver"
s3_output="s3://raw-bucket-walmart/raw_data/"

df=glueContext.read.format("jdbc").option("driver",jdbc_driver_name).option("url",db_url).option("dbtable",table_name).option("user",db_username).option("password",db_password).load()

job.init(args['JOB_NAME'], args)

dynamic_df=DynamicFrame.fromDF(df,glueContext,"dynamic_df")

datasink4= glueContext.write_dynamic_frame.from_options(frame=dynamic_df, connection_type="s3", connection_options={"path": s3_output}, format="csv", transformation_ctx="datasink4")


job.commit()