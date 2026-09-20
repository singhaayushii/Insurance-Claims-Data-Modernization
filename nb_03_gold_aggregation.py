# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.insurancedeprojectsa.dfs.core.windows.net",
    "<STORAGE_ACCOUNT_ACCESS_KEY>"
)

# COMMAND ----------

silver_customers = spark.read.format("delta").load(
    "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/silver/customers"
)

silver_claims = spark.read.format("delta").load(
    "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/silver/claims"
)

# COMMAND ----------

from pyspark.sql.functions import count, sum

claim_summary = silver_claims.groupBy("claim_status").agg(
    count("claim_id").alias("total_claims"),
    sum("claim_amount").alias("total_claim_amount")
)

display(claim_summary)

# COMMAND ----------

claim_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/gold/claim_summary"
    )

# COMMAND ----------

from pyspark.sql.functions import sum, count

city_summary = silver_customers.join(
    silver_claims,
    "customer_id",
    "inner"
).groupBy("city").agg(
    count("claim_id").alias("total_claims"),
    sum("claim_amount").alias("total_claim_amount")
)

display(city_summary)

# COMMAND ----------

city_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/gold/city_summary"
    )

# COMMAND ----------

policy_summary = silver_customers.join(
    silver_claims,
    "customer_id",
    "inner"
).groupBy("policy_type").agg(
    count("customer_id").alias("customer_count"),
    sum("premium_amount").alias("total_premium"),
    sum("claim_amount").alias("total_claim_amount")
)

display(policy_summary)

# COMMAND ----------

policy_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        "abfss://insurance-data@insurancedeprojectsa.dfs.core.windows.net/gold/policy_summary"
    )