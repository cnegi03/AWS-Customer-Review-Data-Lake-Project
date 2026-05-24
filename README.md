# AWS-Customer-Review-Data-Lake-Project
Serverless AWS Data Lake for Customer Review Analytics

# Projects
# AWS Data Lake ETL Pipeline for Customer Reviews

Built a serverless data lake solution using Amazon S3, AWS Glue, and Amazon Athena to process customer review datasets.
Developed AWS Glue ETL jobs to clean, transform, and convert CSV data into optimized Parquet format.
Configured Glue Crawlers and Data Catalog for schema discovery and metadata management.
Queried transformed datasets using Amazon Athena for analytics and reporting.
Implemented partitioning and columnar storage optimization to improve query performance and reduce scan costs.

# Architecture
 AWS Services Used 
Amazon S3
AWS Glue
AWS Glue Crawlers
AWS Athena
AWS Lambda
AWS IAM
CloudWatch Logs

# Project Workflow
Upload raw customer review CSV files to Amazon S3.
AWS Glue Crawler scans raw data and creates metadata tables.
AWS Glue ETL Job cleans and transforms the dataset.
Data is converted from CSV to Parquet format.
Transformed data is stored in another S3 bucket/folder.
Glue Crawler catalogs transformed Parquet data.
Athena queries transformed datasets for analytics.
Lambda trigger ETL automation when new files arrive.


# AWS Glue ETL Script
**customer_review_etl.py**

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1778866404548 = glueContext.create_dynamic_frame.from_catalog(database="demo_db", table_name="iriscsv", transformation_ctx="AmazonS3_node1778866404548")

# Script generated for node Change Schema
ChangeSchema_node1778866366311 = ApplyMapping.apply(frame=AmazonS3_node1778866404548, mappings=[("sepal_length", "double", "sepal_length", "double"), ("sepal_width", "double", "sepal_width", "double"), ("petal_length", "double", "petal_length", "double"), ("petal_width", "double", "petal_width", "double"), ("class", "string", "class", "string"), ("sepal_area", "double", "sepal_area", "double"), ("petal_area", "double", "petal_area", "double")], transformation_ctx="ChangeSchema_node1778866366311")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1778866366311, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1778865627867", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1778866412246 = glueContext.getSink(path="s3://aws-glue0396/iris/parquet/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1778866412246")
AmazonS3_node1778866412246.setCatalogInfo(catalogDatabase="demo_db",catalogTableName="iris_parquet")
AmazonS3_node1778866412246.setFormat("glueparquet", compression="snappy")
AmazonS3_node1778866412246.writeFrame(ChangeSchema_node1778866366311)
job.commit()

# AWS Lambda Trigger

**trigger_lambda.py**

import json
import boto3

client = boto3.client('glue')


def lambda_handler(event, context):
    response = client.start_job_run(
        JobName='customer-review-etl-job'
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Glue ETL Job Triggered Successfully')
    }


# Athena SQL Queries
**analysis_queries.sql**

**-- View all records**
SELECT *
FROM customer_review_processed;

**-- Average rating by product**
SELECT product_name,
       AVG(rating) AS average_rating
FROM customer_review_processed
GROUP BY product_name
ORDER BY average_rating DESC;

**-- Count reviews by rating**
SELECT rating,
       COUNT(*) AS total_reviews
FROM customer_review_processed
GROUP BY rating
ORDER BY rating DESC;

**-- Products with poor ratings**
SELECT product_name,
       rating,
       review_text
FROM customer_review_processed
WHERE rating <= 2;

# Steps to Implement the Project
**Step 1: Create S3 Buckets**
Create two buckets:

customer-review-raw-data
customer-review-processed-data

**Step 2: Upload Raw CSV File**

Upload customer_reviews.csv into:

**Step 3: Create IAM Role**

Attach the following permissions:

AmazonS3FullAccess
AWSGlueServiceRole
AmazonAthenaFullAccess
CloudWatchLogsFullAccess

**Step 4: Create Glue Crawler**

Crawler configuration:

Data source: S3 raw bucket
Database name: customer_review_db
Table name: customer_review_raw

Run crawler.

**Step 5: Create Glue ETL Job**

Upload:

**customer_review_etl.py**

Job configuration:

IAM Role: Glue Service Role
Glue Version: 4.0
Worker Type: G.1X
Run ETL job.

**Step 6: Create Processed Data Crawler**
Configure crawler for:
s3://customer-review-processed-data/transformed/
Create table:
customer_review_processed

**Step 7: Query Data in Athena**
Set Athena result location:
s3://customer-review-query-results/
Run SQL queries.

**Step 8: Configure Lambda Trigger**

Create Lambda function
Add trigger:
S3 PUT Event
Whenever a new CSV file is uploaded, Lambda triggers Glue ETL automatically.

# Conclusion

This project demonstrates a complete end-to-end AWS Data Engineering workflow using serverless AWS services. It showcases ETL processing, data lake implementation, schema management, automation, and analytical querying capabilities.
