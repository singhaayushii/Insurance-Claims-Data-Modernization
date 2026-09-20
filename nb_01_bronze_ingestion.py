# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.insurancedeprojectsa.dfs.core.windows.net",
    "<STORAGE_ACCOUNT_ACCESS_KEY>"
)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/"
    )
)

# COMMAND ----------

df_customers = spark.read.option("header","true").csv(
    "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/bronze/customers/customers.csv"
)

display(df_customers)

# COMMAND ----------

df_claims = spark.read.option("header","true").csv(
    "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/bronze/claims/claims.csv"
)

display(df_claims)

# COMMAND ----------

print("Customers Count:", df_customers.count())
print("Claims Count:", df_claims.count())

# COMMAND ----------

df_customers.printSchema()

df_claims.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum, when

df_customers.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_customers.columns
]).show()

# COMMAND ----------

df_claims.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_claims.columns
]).show()

# COMMAND ----------

print("Total Rows:", df_customers.count())
print("Distinct Rows:", df_customers.distinct().count())

# COMMAND ----------

print("Total Rows:", df_claims.count())
print("Distinct Rows:", df_claims.distinct().count())

# COMMAND ----------

invalid_claims = df_claims.join(
    df_customers,
    "customer_id",
    "left_anti"
)

display(invalid_claims)

# COMMAND ----------

Customers:
- Null customer_name = 1
- Null city = 1
- Duplicate rows = 1

Claims:
- Null claim_type = 1
- Null claim_amount = 1
- Duplicate rows = 1
- Invalid customer_id = 999

# COMMAND ----------

df_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/bronze/customers_delta")

# COMMAND ----------

df_claims.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/bronze/claims_delta")