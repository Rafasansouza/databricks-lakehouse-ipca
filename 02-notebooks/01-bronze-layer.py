# Databricks notebook source
import requests
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, to_date, col, lit, current_timestamp
import uuid

# Criar SparkSession
spark = SparkSession.builder.appName("IPCA_Bronze").getOrCreate()

# Nos requisitos, na terceira etapa, pede-se analise do IPCA nos ultimos 30 anos. Sendo assim, foi definido consultar da data atual aos últimos 30 anos
last_date = datetime.now()
first_date = last_date - timedelta(days=365 * 30)

last_date_str = last_date.strftime("%d/%m/%Y")
first_date_str = "01/01/" + first_date.strftime("%Y")

# Fonte e ID de lote
source_system = "API_BCB_IPCA"
batch_id = str(uuid.uuid4())

# Get na API IPCA do Banco Central com request
url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.10844/dados?formato=json&dataInicial={first_date_str}&dataFinal={last_date_str}"

response = requests.get(url)
response.raise_for_status()  # Informando erro se a api falhar
data = response.json()

# Criar DataFrame Pandas -> Spark
df_bronze_pd = pd.DataFrame(data)

df_bronze_spark = (
    spark.createDataFrame(df_bronze_pd)
    .withColumnRenamed("data", "data")
    .withColumnRenamed("valor", "ipca")
    .withColumn("data", to_date(col("data"), "dd/MM/yyyy"))
    .withColumn("ipca", col("ipca").cast("double"))\
    # Estou adicionando essas colunas para salvar particionado por ano e mes
    .withColumn("ano", year(col("data"))) \
    .withColumn("mes", month(col("data")))
    # Estou incluido metadados para facilitar rastreabilidade e auditória
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit(source_system))
    .withColumn("batch_id", lit(batch_id))
)
# Armazenando os dados na camada Bronze
bronze_path = "/FileStore/tables/case_ip2ca/bronze"

# Armazenar no Delta Lake particionado em ano e mes para otimizar consultas
df_bronze_spark.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("ano", "mes") \
    .save(bronze_path)

print(f"Dados salvos na camada Bronze: {bronze_path}")
