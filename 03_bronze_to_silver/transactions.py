import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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
AmazonS3_node1773865404197 = glueContext.create_dynamic_frame.from_catalog(database="fintech", table_name="transactions", transformation_ctx="AmazonS3_node1773865404197")

# Script generated for node Drop Duplicates
DropDuplicates_node1773865487120 =  DynamicFrame.fromDF(AmazonS3_node1773865404197.toDF().dropDuplicates(["transaction_id"]), glueContext, "DropDuplicates_node1773865487120")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1773865487120, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1773865113144", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1773867903208 = glueContext.getSink(path="s3://fintech-pipeline-raw-263704/silver/transactions/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1773867903208")
AmazonS3_node1773867903208.setCatalogInfo(catalogDatabase="fintech",catalogTableName="silver_transactions")
AmazonS3_node1773867903208.setFormat("glueparquet", compression="snappy")
AmazonS3_node1773867903208.writeFrame(DropDuplicates_node1773865487120)
job.commit()