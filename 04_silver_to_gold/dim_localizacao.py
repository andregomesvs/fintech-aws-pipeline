
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

BUCKET = "s3://fintech-pipeline-raw-263704"
print("✅ Ambiente inicializado!")
#lENDO AS TABELA TABELA DE CLIENTE 
print("Lendo silver_customers... ")
df_customers = spark.read.parquet(f"{BUCKET}/silver/customers/")
print(f" {df_customers.count():,} linhas | {len(df_customers.columns)} colunas")
#CRIANDO A DIM_CLIENTE
from pyspark.sql.functions import *

df_local = df_customers.select(
    col("address_street").alias("ENDERECO"),
    col("address_neighborhood").alias("BAIRRO"),
    col("address_city").alias("CIDADE"),
    col("address_state").alias("UF"),
    col("address_zip").alias("CEP"),
    
)
print(f"{df_local}")
df_local.show()

import pyspark.sql.functions as F

df_local_rmv = (
    df_local
    .dropDuplicates(["CEP"])
    .filter(F.col("CEP").isNotNull())
)
output_path = f"{BUCKET}/gold/dim_localizacao/"

df_local_rmv.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"✅ Salvo em: {output_path}")
print("=" * 55)
job.commit()