# Databricks notebook source
#Criar tabela
spark.sql("""
CREATE TABLE IF NOT EXISTS gold_ipca
USING DELTA
LOCATION '/FileStore/tables/case_ip2ca/gold'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_ipca;
# MAGIC