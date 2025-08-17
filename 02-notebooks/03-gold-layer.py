# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, sum as _sum, round as _round

# Criar SparkSession
spark = SparkSession.builder.appName("IPCA_Gold").getOrCreate()

# Caminhos das camadas
silver_path = "/FileStore/tables/case_ip2ca/silver"
gold_path = "/FileStore/tables/case_ip2ca/gold"

# Ler dados da camada Silver
df_silver = spark.read.format("delta").load(silver_path)

# Calcular IPCA acumulado por ano (percentual e decimal)
df_gold = df_silver.groupBy("ano") \
    .agg(
        _round(_sum("IPCA"), 2).alias("IPCA_Anual_percentual"),
        _round(_sum("IPCA")/100, 4).alias("IPCA_Anual_decimal")
    ) \
    .orderBy("ano")

# Armazenando dados na camada gold
df_gold.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path)

print(f"Dados salvos na camada Gold: {gold_path}")