# Databricks notebook source
# Create volumes if they do not exist (run once)
spark.sql(
    "CREATE VOLUME IF NOT EXISTS workspace.default.transactions_bronze"
)
spark.sql(
    "CREATE VOLUME IF NOT EXISTS workspace.default.transactions_silver"
)
spark.sql(
    "CREATE VOLUME IF NOT EXISTS workspace.default.transactions_gold"
)

# Use the Unity Catalog volume paths for your data
bronze_path = "/Volumes/workspace/default/transactions_bronze"
silver_path = "/Volumes/workspace/default/transactions_silver"
gold_path = "/Volumes/workspace/default/transactions_gold"


# COMMAND ----------

import requests
import json
from datetime import datetime
import os

# COMMAND ----------

url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"

params = {
    "vs_currency": "usd",
    "days": 1
}

response = requests.get(url, params=params)

if response.status_code != 200:
    raise Exception(f"API call failed with status {response.status_code}")

api_data = response.json()

# COMMAND ----------


bronze_record = {
    "ingestion_timestamp": datetime.utcnow().isoformat(),
    "source_system": "coingecko_api",
    "entity": "bitcoin_transactions",
    "payload": api_data
}


# COMMAND ----------

file_name = f"transactions_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
file_path = f"{bronze_path}/{file_name}"

with open(file_path, "w") as f:
    json.dump(bronze_record, f)

file_path


# COMMAND ----------

dbutils.fs.ls(bronze_path)
display(spark.read.json(bronze_path))

# COMMAND ----------

bronze_path = "/Volumes/workspace/default/transactions_bronze"

df_bronze = spark.read.json(bronze_path)

df_bronze.show(truncate=False)


# COMMAND ----------

df_bronze.printSchema

# COMMAND ----------

from pyspark.sql.functions import col, explode

df_prices = df_bronze.select(
    col("ingestion_timestamp"),
    col("source_system"),
    explode(col("payload.prices")).alias("price_record")
)

# COMMAND ----------

df_prices_flat = df_prices.select(
    col("ingestion_timestamp"),
    col("source_system"),
    col("price_record")[0].alias("event_timestamp"),
    col("price_record")[1].alias("price_usd")
)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime

df_prices_clean = df_prices_flat.withColumn(
    "event_time",
    from_unixtime(col("event_timestamp") / 1000)
).drop("event_timestamp")

# COMMAND ----------

df_valid = df_prices_clean.filter(col("price_usd").isNotNull())
df_valid = df_valid.filter(col("price_usd") >= 0)


# COMMAND ----------

silver_path = "/Volumes/workspace/default/transactions_silver"

df_valid.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)
    

# COMMAND ----------

spark.read.format("delta").load(silver_path).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #########GOLDEN LAYER######

# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path)
df_silver.show()


# COMMAND ----------

# MAGIC %md
# MAGIC For Gold Layer, We’ll compute:
# MAGIC 	•	Average price
# MAGIC 	•	Minimum price
# MAGIC 	•	Maximum price
# MAGIC 	•	Transaction count
# MAGIC 	•	Aggregated by date

# COMMAND ----------

from pyspark.sql.functions import to_date

df_with_date = df_silver.withColumn(
    "event_date", to_date("event_time")
)

# COMMAND ----------

from pyspark.sql.functions import avg, min, max, count

df_gold = df_with_date.groupBy("event_date").agg(
    avg("price_usd").alias("avg_price_usd"),
    min("price_usd").alias("min_price_usd"),
    max("price_usd").alias("max_price_usd"),
    count("*").alias("transaction_count")
)


# COMMAND ----------

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/Volumes/workspace/default/transactions_gold")

# COMMAND ----------

spark.read.format("delta").load("/Volumes/workspace/default/transactions_gold").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating SQL Views

# COMMAND ----------

df_gold.createOrReplaceTempView("vw_daily_price_metrics")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM vw_daily_price_metrics
# MAGIC ORDER BY event_date;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC INCREMENTAL LOAD + IDEMPOTENCY (Mini CDC)
# MAGIC “The pipeline is idempotent. Re-running it does not create duplicates.”

# COMMAND ----------

from delta.tables import DeltaTable

silver_delta = DeltaTable.forPath(spark, silver_path)


# COMMAND ----------

df_valid_dedup = (
    df_valid
    .dropDuplicates(["event_time", "source_system"])
)

(
    silver_delta.alias("t")
    .merge(
        df_valid_dedup.alias("s"),
        "t.event_time = s.event_time AND t.source_system = s.source_system"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import lit

df_with_new_col = df_valid.withColumn("currency", lit("USD"))

# COMMAND ----------

(
    df_with_new_col.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(silver_path)
)

# COMMAND ----------

df_check.select("event_time", "price_usd", "currency").show(10)

# COMMAND ----------

from delta.tables import DeltaTable

silver_delta = DeltaTable.forPath(spark, silver_path)
silver_delta.history().show(truncate=False)

# COMMAND ----------

