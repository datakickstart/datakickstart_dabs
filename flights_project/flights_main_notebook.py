# Databricks notebook source
# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("database", "datakickstart_dev")
# COMMAND ----------

# MAGIC %md
# MAGIC ## Read csv data (batch mode)
# COMMAND ----------

# DBTITLE 1,Setup vars and functions
from flights.transforms import flight_transforms, shared_transforms
catalog = dbutils.widgets.get("catalog")
database = dbutils.widgets.get("database")

path = "/databricks-datasets/airlines"
raw_table_name = f"{catalog}.{database}.flights_raw"

# def write_to_delta(df, dest_table, checkpoint_location):
#   df.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_location).toTable(dest_table)


# COMMAND ----------

# DBTITLE 1,Read raw
df = flight_transforms.read_batch(spark, path).limit(1000)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write raw Delta Lake table (batch mode)

# COMMAND ----------
df.write.format("delta").mode("append").saveAsTable(raw_table_name)
# shared_transforms.append_to_delta(df, raw_table_name)


# COMMAND ----------
