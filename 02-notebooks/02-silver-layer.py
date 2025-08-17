# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, sum as _sum, year, month
from pyspark.sql.window import Window

# Criar SparkSession
spark = SparkSession.builder.appName("IPCA_Silver").getOrCreate()

# Caminhos
bronze_path = "/FileStore/tables/case_ip2ca/bronze"
silver_path = "/FileStore/tables/case_ip2ca/silver"

# Lendo a camada bronze
df_bronze = spark.read.format("delta").load(bronze_path)

# 2. Limpeza e padronização
df_silver = df_bronze \
    .filter(col("ipca").isNotNull()) \
    .dropDuplicates(["data"]) \
    .orderBy("data")

# Armazenando na camada silver particionado em ano e mes para otimizar consultas
df_silver.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("ano", "mes") \
    .save(silver_path)

print(f"Dados salvos na camada Silver: {silver_path}")


# COMMAND ----------

