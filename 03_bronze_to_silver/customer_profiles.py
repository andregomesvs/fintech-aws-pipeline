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

# Script generated for node Ext_costumer_profile
Ext_costumer_profile_node1773869876780 = glueContext.create_dynamic_frame.from_catalog(database="fintech", table_name="customer_profiles", transformation_ctx="Ext_costumer_profile_node1773869876780")

# Script generated for node Drop Duplicates
DropDuplicates_node1773869923750 =  DynamicFrame.fromDF(Ext_costumer_profile_node1773869876780.toDF().dropDuplicates(["cpf", "card_number"]), glueContext, "DropDuplicates_node1773869923750")

# Script generated for node TRF_ACCOUNT_OPEN_DATE
TRF_ACCOUNT_OPEN_DATE_node1773870180096 = DropDuplicates_node1773869923750.gs_derived(colName="to_timestamp(from_unixtime( account_open_date/ 1000000000))", expr="account_open_date")

# Script generated for node INSERT_SILVER_COSTUMERS_PROFILE
EvaluateDataQuality().process_rows(frame=TRF_ACCOUNT_OPEN_DATE_node1773870180096, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1773869842130", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
INSERT_SILVER_COSTUMERS_PROFILE_node1773870294281 = glueContext.getSink(path="s3://fintech-pipeline-raw-263704/silver/customers_profile/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="INSERT_SILVER_COSTUMERS_PROFILE_node1773870294281")
INSERT_SILVER_COSTUMERS_PROFILE_node1773870294281.setCatalogInfo(catalogDatabase="fintech",catalogTableName="silver_costumers_profile")
INSERT_SILVER_COSTUMERS_PROFILE_node1773870294281.setFormat("glueparquet", compression="snappy")
INSERT_SILVER_COSTUMERS_PROFILE_node1773870294281.writeFrame(TRF_ACCOUNT_OPEN_DATE_node1773870180096)
job.commit()