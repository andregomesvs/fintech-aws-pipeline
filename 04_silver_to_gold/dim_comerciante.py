
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
# ── Lê silver_transactions ─────────────────────────────────
print("Lendo silver_transactions...")
df_transactions = spark.read.parquet(f"{BUCKET}/silver/transactions/")
print(f"  {df_transactions.count():,} linhas")
#CRIANDO A DIM_CARTAO 
from pyspark.sql import functions as F

dim_cartao = (df_customers_prof
    .select(
        "cpf",
        "card_tier",
        "card_flag",
        "card_number",
        "total_credit_limit",
        "available_credit_limit",
        "used_credit_limit",
        "credit_score",
        "payment_day",
        "digital_banking",
        "account_type",
        "account_open_date"
    )
    # Gera chave surrogate
    .withColumn("card_id",
        F.md5(F.concat(F.col("cpf"), F.lit("_"), F.col("card_tier"))))
             )
              
print(f"\n✅ dim_cartao: {dim_cartao.count():,} linhas | {len(dim_cartao.columns)} colunas")
dim_cartao = (
    dim_cartao.dropDuplicates(["cpf"])
    .filter(F.col("cpf").isNotNull())
)

print(f"\n✅ dim_cartao: {dim_cartao.count():,} linhas | {len(dim_cartao.columns)} colunas")
dim_cartao.limit(5).toPandas()
output_path = f"{BUCKET}/gold/dim_cartao/"

dim_cartao.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"\n✅ Salvo em: {output_path}")
print("=" * 55)

job.commit()