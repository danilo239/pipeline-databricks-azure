# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC # Conferindo se os dados foram montados e se temos acesso à pasta inbound

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dados/inbound'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Lendo os dados na camada inbound

# COMMAND ----------

path = 'dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json'
dados = spark.read.json(path)

# COMMAND ----------

display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC # Removendo colunas

# COMMAND ----------

dados = dados.drop('imagens', 'usuario')

# COMMAND ----------

display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC #Criando uma coluna de identificação

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_bronze = dados.withColumn('id', col('anuncio.id'))
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando na camada bronze

# COMMAND ----------

path = 'dbfs:/mnt/dados/bronze/dataset_imoveis'
df_bronze.write.format("delta").mode("overwrite").save(path)
