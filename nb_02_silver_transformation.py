# Databricks notebook source
df_customers = spark.read.format("delta").load(
    "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/bronze/customers_delta"
)

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.insurancedeprojectsa.dfs.core.windows.net",
    "<STORAGE_ACCOUNT_ACCESS_KEY>"
)

# COMMAND ----------

df_claims = spark.read.format("delta").load(
    "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/bronze/claims_delta"
)

# COMMAND ----------

df_customers = df_customers.dropDuplicates()

# COMMAND ----------

df_claims = df_claims.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import col, when

df_customers = df_customers.fillna({
    "customer_name": "Unknown",
    "city": "Unknown"
})

# COMMAND ----------

df_claims = df_claims.fillna({
    "claim_type": "Unknown",
    "claim_amount": 0
})

# COMMAND ----------

df_claims = df_claims.join(
    df_customers.select("customer_id"),
    "customer_id",
    "inner"
)

# COMMAND ----------

from pyspark.sql.functions import col

df_customers = df_customers.withColumn(
    "premium_amount",
    col("premium_amount").cast("int")
)

# COMMAND ----------

df_claims = df_claims.withColumn(
    "claim_amount",
    col("claim_amount").cast("int")
)

# COMMAND ----------

print("Customers:", df_customers.count())
print("Claims:", df_claims.count())

# COMMAND ----------

display(df_customers)
display(df_claims)

# COMMAND ----------

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/silver/customers"
    )

# COMMAND ----------

df_claims.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/silver/claims"
    )