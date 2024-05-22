# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

schema = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/databricks-datasets/airlines/part-00000").limit(100).schema
schema.simpleString()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python functions to test
# MAGIC These represent Python functions that you would keep in a Python file and import to test. For simplicity I have kept this all in one notebook for this example.

# COMMAND ----------

# DBTITLE 1,Python functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def read_autoloader(path, checkpoint_location):
  schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Month", IntegerType(), True),
      StructField("DayofMonth", IntegerType(), True),
      StructField("DayOfWeek", IntegerType(), True),
      StructField("DepTime", StringType(), True),
      StructField("CRSDepTime", IntegerType(), True),
      StructField("ArrTime", StringType(), True),
      StructField("CRSArrTime", IntegerType(), True),
      StructField("UniqueCarrier", StringType(), True),
      StructField("FlightNum", IntegerType(), True),
      StructField("TailNum", StringType(), True),
      StructField("ActualElapsedTime", StringType(), True),
      StructField("CRSElapsedTime", IntegerType(), True),
      StructField("AirTime", StringType(), True),
      StructField("ArrDelay", StringType(), True),
      StructField("DepDelay", StringType(), True),
      StructField("Origin", StringType(), True),
      StructField("Dest", StringType(), True),
      StructField("Distance", StringType(), True),
      StructField("TaxiIn", StringType(), True),
      StructField("TaxiOut", StringType(), True),
      StructField("Cancelled", IntegerType(), True),
      StructField("CancellationCode", StringType(), True),
      StructField("Diverted", IntegerType(), True),
      StructField("CarrierDelay", StringType(), True),
      StructField("WeatherDelay", StringType(), True),
      StructField("NASDelay", StringType(), True),
      StructField("SecurityDelay", StringType(), True),
      StructField("LateAircraftDelay", StringType(), True),
      StructField("IsArrDelayed", StringType(), True),
      StructField("IsDepDelayed", StringType(), True)
  ])
  
  streaming_df = spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", "csv") \
      .option("cloudFiles.includeExistingFiles", "true") \
      .schema(schema) \
      .load(path)

      # .option("cloudFiles.inferSchema", "true") \
      # .option("cloudFiles.schemaLocation", checkpoint_location+"/schema") \
  
  return streaming_df
      

def write_to_delta(df, dest_table, checkpoint_location):
  df.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_location).toTable(dest_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test code
# MAGIC This code is used to test the functions. We don't test the writer here but you could always add more test cases which write to a temporary location to test the writer.

# COMMAND ----------

# %fs ls dbfs:/tmp/dab_demos/test_csv

# COMMAND ----------

# DBTITLE 1,Reset source data
QUERY_NAME = "streaming_unit_test"
SOURCE_PATH = "dbfs:/tmp/dab_demos/test_csv"
CHECKPOINT_LOCATION = "dbfs:/tmp/dab_demos/test_streaming_autloader_checkpoint"

dbutils.fs.rm(SOURCE_PATH, recurse=True)

dbutils.fs.rm(CHECKPOINT_LOCATION, recurse=True)

dbutils.fs.mkdirs("dbfs:/tmp/dab_demos/test_csv")

# COMMAND ----------

import pytest
import sys

def run_pytest(pytest_path):
  # Skip writing pyc files on a readonly filesystem.
  sys.dont_write_bytecode = True

  retcode = pytest.main([pytest_path, "-p", "no:cacheprovider"])

  # Fail the cell execution if we have any test failures.
  assert retcode == 0, 'The pytest invocation failed. See the log above for details.'

# COMMAND ----------

# Test records
header = """Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed,IsDepDelayed\n"""

text1 = """1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,YES,YES\n
1987,10,15,4,729,730,903,849,PS,1451,NA,94,79,NA,14,-1,SAN,SFO,447,NA,NA,0,NA,0,NA,YES,NA,NA,NA,YES,NO"""

text2 = """1987,10,17,6,741,730,918,849,PS,1451,NA,97,79,NA,29,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,YES,YES\n
1987,10,18,7,729,730,847,849,PS,1451,NA,78,79,NA,-2,-1,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,NO,NO"""

text3 = """1987,10,19,1,749,730,922,849,PS,1451,NA,93,79,NA,33,19,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,YES,NA,NA,YES,YES"""

# COMMAND ----------

# DBTITLE 1,Test utility function

def write_source_batch(part, text):
  with open(f"/dbfs/tmp/dab_demos/test_csv/{part}.csv", 'w') as f:
    f.write(text)

def write_to_memory(df, query_name):
  query = (df
      .writeStream
      .format("memory")
      .queryName(query_name)
      .start()
  )
  return query

# COMMAND ----------

# DBTITLE 1,Start test stream
streaming_df = read_autoloader(SOURCE_PATH, CHECKPOINT_LOCATION)
query = write_to_memory(streaming_df, QUERY_NAME)

# COMMAND ----------

import time

def test_before_write():
  assert(spark.table(QUERY_NAME).count() == 0)

def test_first_write():
  write_source_batch("part1", text1)
  time.sleep(30)
  assert(spark.table(QUERY_NAME).count() == 2)

def test_second_write():
  write_source_batch("part2", text2)
  time.sleep(30)
  assert(spark.table(QUERY_NAME).count() == 4)

# COMMAND ----------

test_before_write()

# COMMAND ----------

test_first_write()

# COMMAND ----------

test_second_write()

# COMMAND ----------

display(spark.table(QUERY_NAME))

# COMMAND ----------

query.stop()
