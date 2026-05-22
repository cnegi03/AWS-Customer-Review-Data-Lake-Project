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
ChangeSchema_node1778866366311 = ApplyMapping.apply(frame=AmazonS3_node1778866404548, mappings=[("sepal_length", "double", "sepal_length", "double"), ("sepal_width", "double", "sepal_width", "double"), ("petal_length", "double", "petal_length", "double"), ("petal_width", "double", "petal_width", "double"), ("class", "string", "class", "string")], transformation_ctx="ChangeSchema_node1778866366311")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1778866366311, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1778865627867", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1778866412246 = glueContext.getSink(path="s3://aws-glue0396/iris/parquet/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1778866412246")
AmazonS3_node1778866412246.setCatalogInfo(catalogDatabase="demo_db",catalogTableName="iris_parquet")
AmazonS3_node1778866412246.setFormat("glueparquet", compression="snappy")
AmazonS3_node1778866412246.writeFrame(ChangeSchema_node1778866366311)
job.commit()