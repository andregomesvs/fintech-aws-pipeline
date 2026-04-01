
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
print("Lendo tabelas...")

df_tx   = spark.read.parquet(f"{BUCKET}/silver/transactions/")
dim_tmp = spark.read.parquet(f"{BUCKET}/gold/dim_tempo/")
dim_car = spark.read.parquet(f"{BUCKET}/gold/dim_cartao/")
dim_com = spark.read.parquet(f"{BUCKET}/gold/dim_comerciante/")

print(f"  transactions:   {df_tx.count():,}")
print(f"  dim_tempo:      {dim_tmp.count():,}")
print(f"  dim_cartao:     {dim_car.count():,}")
print(f"  dim_comerciante:{dim_com.count():,}")
#CRIANDO A DIM_CARTAO 
from pyspark.sql import functions as F

df_tx = df_tx \
    .withColumn("date_id",
        F.date_format("purchase_date", "yyyyMMdd").cast("int")) \
    .withColumn("merchant_id",
        F.md5(F.concat(
            F.col("merchant_name"), F.lit("_"),
            F.col("merchant_category"), F.lit("_"),
            F.col("merchant_country")
        )))

print("✅ date_id e merchant_id gerados!")
dim_car_key = dim_car.select("cpf", "card_id")

df_tx = df_tx.join(dim_car_key, on="cpf", how="left")

print(f"✅ card_id vinculado: {df_tx.count():,} linhas")
dim_cartao.limit(5).toPandas()
fat_transacoes = df_tx.select(
    "transaction_id",
    "date_id",
    "cpf",
    "card_id",
    "merchant_id",
    "amount_brl",
    "amount",
    "currency",
    "payment_type",
    "installment_count",
    "installment_amount",
    "status",
    "is_fraud_flag",
    "is_international",
    "decline_reason"
)

print(f"✅ fat_transacoes: {fat_transacoes.count():,} linhas | {len(fat_transacoes.columns)} colunas")
fat_transacoes.show(5)
fat_transacoes = fat_transacoes \
    .withColumn("year",  F.col("date_id").cast("string").substr(1, 4)) \
    .withColumn("month", F.col("date_id").cast("string").substr(5, 2))

fat_transacoes.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"{BUCKET}/gold/fat_transacoes/")

print("✅ fat_transacoes salva particionada por year/month!")
# ── dim_status ─────────────────────────────────────────────
print("Criando dim_status...")

dim_status = df_tx.select(
    "status",
    "decline_reason"
).dropDuplicates(["status", "decline_reason"]) \
 .filter(F.col("status").isNotNull()) \
 .withColumn("status_id",
    F.md5(F.concat(
        F.col("status"),
        F.lit("_"),
        F.coalesce(F.col("decline_reason"), F.lit("N/A"))
    ))) \
 .withColumn("is_approved",
    F.col("status") == "approved") \
 .withColumn("is_declined",
    F.col("status") == "declined") \
 .withColumn("is_reversed",
    F.col("status") == "reversed") \
 .withColumn("decline_reason",
    F.coalesce(F.col("decline_reason"), F.lit("N/A"))) \
 .select(
    "status_id",
    "status",
    "decline_reason",
    "is_approved",
    "is_declined",
    "is_reversed"
 )

print(f"✅ dim_status: {dim_status.count():,} linhas")
dim_status.show(truncate=False)
# ── dim_pagamento ──────────────────────────────────────────
print("Criando dim_pagamento...")

df_tx = spark.read.parquet(f"{BUCKET}/silver/transactions/")

dim_pagamento = df_tx.select(
    "payment_type",
    "installment_count"
).dropDuplicates(["payment_type", "installment_count"]) \
 .filter(F.col("payment_type").isNotNull()) \
 .withColumn("payment_id",
    F.md5(F.concat(
        F.col("payment_type"),
        F.lit("_"),
        F.col("installment_count").cast("string")
    ))) \
 .withColumn("is_installment",
    F.col("payment_type") == "installment") \
 .withColumn("installment_range",
    F.when(F.col("installment_count") == 1,  "a_vista")
    .when(F.col("installment_count") <= 3,  "ate_3x")
    .when(F.col("installment_count") <= 6,  "ate_6x")
    .when(F.col("installment_count") <= 12, "ate_12x")
    .otherwise("acima_12x")) \
 .select(
    "payment_id",
    "payment_type",
    "installment_count",
    "installment_range",
    "is_installment"
 )

print(f"✅ dim_pagamento: {dim_pagamento.count():,} linhas")
dim_pagamento.show(truncate=False)
dim_pagamento.write \
    .mode("overwrite") \
    .parquet(f"{BUCKET}/gold/dim_pagamento/")
print("✅ dim_pagamento salva!")

dim_status.write \
    .mode("overwrite") \
    .parquet(f"{BUCKET}/gold/dim_status/")
print("✅ dim_status salva!")
job.commit()