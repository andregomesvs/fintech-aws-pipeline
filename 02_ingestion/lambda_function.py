import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
import logging
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "fintech-pipeline-raw-263704")
PROCESSED_PREFIX =  os.environ.get("PROCESSED_PREFIX", "bronze/")
CHUNK_SIZE = 50_000  # linhas por chunk

TABLE_MAP = {
    "customers":         "customers",
    "customer":         "customers",
    "customer_profiles": "customer_profiles",
    "transactions":      "transactions",
}

def detect_table(key):
    for keyword, table in TABLE_MAP.items():
        if keyword in key:
            return table
    return "unknown"

def lambda_handler(event, context):
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key    = unquote_plus(record["s3"]["object"]["key"])

        logger.info(f"Arquivo recebido: s3://{bucket}/{key}")

        if not key.startswith("raw/") or not key.endswith(".csv"):
            logger.info(f"Ignorando: {key}")
            continue

        table_name = detect_table(key)
        logger.info(f"Tabela detectada: {table_name}")

        # ── Baixa o arquivo para /tmp ────────────────────────
        local_path = f"/tmp/{key.split('/')[-1]}"
        logger.info(f"Baixando para {local_path}...")
        s3.download_file(bucket, key, local_path)
        logger.info("Download concluido!")

        # ── Define o caminho de saída ────────────────────────
        filename = key.split("/")[-1].replace(".csv", ".parquet")
        out_key  = f"{PROCESSED_PREFIX}{table_name}/{filename}"

        # ── Lê em chunks e salva Parquet ─────────────────────
        parquet_path = f"/tmp/output_{table_name}.parquet"
        writer = None
        total_rows = 0

        try:
            for chunk in pd.read_csv(local_path, encoding="utf-8-sig", chunksize=CHUNK_SIZE):
                # Tipagem por tabela
                if table_name == "transactions":
                    chunk["purchase_date"]    = pd.to_datetime(chunk["purchase_date"], errors="coerce")
                    chunk["amount"]           = pd.to_numeric(chunk["amount"],         errors="coerce")
                    chunk["amount_brl"]       = pd.to_numeric(chunk["amount_brl"],     errors="coerce")
                    chunk["is_international"] = chunk["is_international"].astype(str).str.lower() == "true"
                    chunk["is_fraud_flag"]    = chunk["is_fraud_flag"].astype(str).str.lower()    == "true"
                elif table_name == "customers":
                    chunk["birth_date"]     = pd.to_datetime(chunk["birth_date"],     errors="coerce")
                    chunk["customer_since"] = pd.to_datetime(chunk["customer_since"], errors="coerce")
                    chunk["monthly_income"] = pd.to_numeric(chunk["monthly_income"],  errors="coerce")
                    chunk["is_active"]      = chunk["is_active"].astype(str).str.lower() == "true"
                elif table_name == "customer_profiles":
                    chunk["account_open_date"]       = pd.to_datetime(chunk["account_open_date"],       errors="coerce")
                    chunk["total_credit_limit"]      = pd.to_numeric(chunk["total_credit_limit"],       errors="coerce")
                    chunk["available_credit_limit"]  = pd.to_numeric(chunk["available_credit_limit"],   errors="coerce")
                    chunk["used_credit_limit"]       = pd.to_numeric(chunk["used_credit_limit"],        errors="coerce")
                    chunk["credit_score"]            = pd.to_numeric(chunk["credit_score"],             errors="coerce")
                    chunk["digital_banking"]         = chunk["digital_banking"].astype(str).str.lower() == "true"

                from datetime import datetime, timezone
                chunk["_ingested_at"] = datetime.now(timezone.utc).isoformat()
                chunk["_source_file"] = key

                table = pa.Table.from_pandas(chunk)

                if writer is None:
                    writer = pq.ParquetWriter(parquet_path, table.schema, compression="snappy")

                writer.write_table(table)
                total_rows += len(chunk)
                logger.info(f"  chunk processado: {total_rows:,} linhas acumuladas")

        finally:
            if writer:
                writer.close()

        # ── Upload do Parquet para o S3 ──────────────────────
        logger.info(f"Fazendo upload para s3://{PROCESSED_BUCKET}/{out_key}")
        s3.upload_file(parquet_path, PROCESSED_BUCKET, out_key)

        # ── Tamanhos ─────────────────────────────────────────
        size_csv     = os.path.getsize(local_path)     / 1024 / 1024
        size_parquet = os.path.getsize(parquet_path)   / 1024 / 1024
        reducao      = (1 - size_parquet / size_csv)   * 100

        logger.info(
            f"Conversao concluida | "
            f"CSV: {size_csv:.1f}MB → Parquet: {size_parquet:.1f}MB | "
            f"Reducao: {reducao:.0f}% | "
            f"Total linhas: {total_rows:,}"
        )

    return {"statusCode": 200, "body": "Processado com sucesso"}