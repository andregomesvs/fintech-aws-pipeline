import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import gs_derived
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

# Script generated for node Ext_customers
Ext_customers_node1773857147025 = glueContext.create_dynamic_frame.from_catalog(database="fintech", table_name="customers", transformation_ctx="Ext_customers_node1773857147025")

# Script generated for node Rmv Duplicados
RmvDuplicados_node1773858583819 =  DynamicFrame.fromDF(Ext_customers_node1773857147025.toDF().dropDuplicates(), glueContext, "RmvDuplicados_node1773858583819")

# Script generated for node Change Schema
ChangeSchema_node1773861957114 = ApplyMapping.apply(frame=RmvDuplicados_node1773858583819, mappings=[("cpf", "long", "cpf", "long"), ("full_name", "string", "full_name", "string"), ("birth_date", "long", "birth_date", "long"), ("gender", "string", "gender", "string"), ("phone", "string", "phone", "string"), ("email", "string", "email", "string"), ("address_street", "string", "address_street", "string"), ("address_neighborhood", "string", "address_neighborhood", "string"), ("address_city", "string", "address_city", "string"), ("address_state", "string", "address_state", "string"), ("address_zip", "string", "address_zip", "string"), ("income_range", "string", "income_range", "string"), ("monthly_income", "double", "monthly_income", "double"), ("occupation", "string", "occupation", "string"), ("customer_since", "long", "customer_since", "long"), ("is_active", "boolean", "is_active", "boolean"), ("_ingested_at", "string", "_ingested_at", "string"), ("_source_file", "string", "_source_file", "string")], transformation_ctx="ChangeSchema_node1773861957114")

# Script generated for node Derived Column
DerivedColumn_node1773862006502 = ChangeSchema_node1773861957114.gs_derived(colName="birth_date", expr="to_timestamp(from_unixtime(birth_date / 1000000000))")

# Script generated for node Derived Column
DerivedColumn_node1773862174397 = DerivedColumn_node1773862006502.gs_derived(colName="age", expr="floor(datediff(current_date(), to_date(birth_date)) / 365)")

# Script generated for node TRF_customer_since
TRF_customer_since_node1774242861216 = DerivedColumn_node1773862174397.gs_derived(colName="customer_since", expr="to_timestamp(from_unixtime(customer_since/ 1000000000))")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=TRF_customer_since_node1774242861216, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1773860390125", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1773862330968 = glueContext.getSink(path="s3://fintech-pipeline-raw-263704/silver/customers/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1773862330968")
AmazonS3_node1773862330968.setCatalogInfo(catalogDatabase="fintech",catalogTableName="silver_customers")
AmazonS3_node1773862330968.setFormat("glueparquet", compression="snappy")
AmazonS3_node1773862330968.writeFrame(TRF_customer_since_node1774242861216)
job.commit()