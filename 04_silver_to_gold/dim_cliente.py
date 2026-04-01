
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

df_cust = df_customers.select(
    col("cpf").alias("CPF"),
    col("full_name").alias("NOME_CLIENTE"),
    to_date(col("birth_date"), "yyyy-MM-dd").alias("DT_NASC"),
    col("age").alias("IDADE"),
    col("monthly_income").alias("SALARIO"),
    col("occupation").alias("OCUPACAO"),
    to_date(col("customer_since"),"yyyy-mm-dd").alias("DT_ABERTURA_CC"),
    col("is_active").alias("INDICADOR_ATIVO")
)
print(f"{df_cust}")
df_cust.show()

import pyspark.sql.functions as F

dim_cliente = (
    df_cust
    .dropDuplicates(["cpf"])
    .filter(F.col("cpf").isNotNull())
)
output_path = f"{BUCKET}/gold/dim_cliente/"

dim_cliente.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"✅ Salvo em: {output_path}")
print("=" * 55)
job.commit()
