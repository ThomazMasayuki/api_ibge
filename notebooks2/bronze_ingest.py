import time
import random
import requests

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SIDRA_URL = "https://apisidra.ibge.gov.br/values/t/6579/n3/all/p/all/v/all"


def get_json_with_retries(url, max_retries=5, timeout=60):
    last_error = None

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=timeout)
            # Se responder correto, continuar
            if response.status_code == 200:
                return response.json()
            # Se responder errado, dizer qual tipo de erro 
            if response.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {response.status_code}")

            response.raise_for_status()

        except Exception as e:
            last_error = e
            time.sleep((2 ** attempt) + random.random())

    raise RuntimeError(f"Falha ao acessar API após {max_retries} tentativas: {last_error}")


def run(spark: SparkSession, table_name: str):
    payload = get_json_with_retries(SIDRA_URL)

    ingestion_ts = datetime.now(timezone.utc).isoformat()
    load_date = ingestion_ts[:10]

    df = spark.createDataFrame(payload)

    df = (
        df.withColumn("_ingestion_ts", F.lit(ingestion_ts))
          .withColumn("_load_date", F.lit(load_date))
          .withColumn("_source_url", F.lit(SIDRA_URL))
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    (
        df.write
          .format("delta")
          .mode("append")
          .partitionBy("_load_date")
          .saveAsTable(table_name)
    )

# Parte de verificação

run(spark, "bronze.sidra_6579_raw")

DESCRIBE EXTENDED bronze.sidra_6579_raw;

df = spark.table("bronze.sidra_6579_raw")
df.printSchema()

SELECT COUNT(*) FROM bronze.sidra_6579_raw;

SELECT D1C, D2N, V
FROM bronze.sidra_6579_raw
LIMIT 20;

SELECT _ingestion_ts, _load_date, _source_url
FROM bronze.sidra_6579_raw
LIMIT 5;

# Transformando a base para na camada bronze
df_bronze = spark.table("bronze.sidra_6579_raw")
display(df_bronze)
