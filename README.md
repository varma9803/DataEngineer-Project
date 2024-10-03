**Description:** 
Creating Data Pipelines to perform ETL operation using walmart data set.

**Tunneling tool:**  
Tunneling tool I have used to establish connection between MySql and S3 using glue job is "ngrok"

**Description of files in order:**
**1) Glue job for walmart.py:** This is glue job script file used to extract data from MySQL to AWS S3 (extraction layer / bronze layer).  
**2) walmart_transformation_notebook.ipynb:** This is a databricks notebook file where few queries of transformation operation are written in spark (results can be viewed in this notebook)  
**3) walmart_transformation_notebook.py:** This is similar databricks notebook saved in python to check the code.  
**4) Transformed Data from databricks to S3:** The following csv file is the transformed file stored in S3 (silver layer).   
**5) SQL configuration for snowflake:** SQl commands written in snowflake to load data from S3 to snowflake warehouse (loading layer / gold layer)  
**6) Data stored in Snowflake warehouse:** This is downloaded file from snowflake warehouse showing the output.

**Additional files:**  
**1) Netflix_analysis_Spark.py:** This is the python notebook file where queries are written in **spark sql** .  
**2) Netflix_analysis_Spark.ipynb:** It is a similar python file in notebook format with results saved to be viewed.  
**3) Netflix output data file:** It is a output file that is saved in **S3 which is further processed to Athena for querying by running glue crawlers.**  
**4) Parquet netflix output data file:** This is the same output file saved in parquet format
