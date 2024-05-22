"""Test code
This code is used to test the functions. We don't test the writer here but you could always add more test cases which write to a temporary location to test the writer.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

import time
import os
import sys
import shutil

currentdir = os.path.dirname(__file__)
parentdir = os.path.dirname(currentdir)
parent_parent_dir =  os.path.dirname(parentdir)
print(parent_parent_dir)
sys.path.insert(0,parent_parent_dir)

from src.datakickstart_streaming.streaming_functions import read_autoloader, delay_type_transform

QUERY_NAME = "streaming_unit_test"

CHECKPOINT_LOCATION = "dbfs:/tmp/dab_demos/test_streaming_autloader_checkpoint"
CHECKPOINT_LOCATION_LOCAL = "/dbfs/tmp/dab_demos/test_streaming_autloader_checkpoint"

SOURCE_PATH = "dbfs:/tmp/dab_demos/test_csv"
SOURCE_PATH_LOCAL = "/dbfs/tmp/dab_demos/test_csv"

global IS_LOCAL
IS_LOCAL = True

# def get_spark():
#   global spark
#   spark = SparkSession.builder.getOrCreate()
#   return spark

def get_spark():
  try:
    from databricks.connect import DatabricksSession
    IS_LOCAL = False
    return DatabricksSession.builder.getOrCreate()
  except ImportError:
    return SparkSession.builder.getOrCreate()

header = """Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed,IsDepDelayed\n"""

text1 = """1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,YES,YES\n
1987,10,15,4,729,730,903,849,PS,1451,NA,94,79,NA,14,-1,SAN,SFO,447,NA,NA,0,NA,0,NA,YES,NA,NA,NA,YES,NO"""

text2 = """1987,10,17,6,741,730,918,849,PS,1451,NA,97,79,NA,29,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,YES,YES\n
1987,10,18,7,729,730,847,849,PS,1451,NA,78,79,NA,-2,-1,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,NO,NO"""

text3 = """1987,10,19,1,749,730,922,849,PS,1451,NA,93,79,NA,33,19,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,YES,NA,NA,YES,YES"""

def setup():
  #remove dirs if exist 
  try:
    shutil.rmtree(SOURCE_PATH_LOCAL)
  except FileNotFoundError:
    pass
  
  try:
    shutil.rmtree(CHECKPOINT_LOCATION_LOCAL)
  except FileNotFoundError:
    pass

  # create source location
  os.makedirs(SOURCE_PATH_LOCAL)

def write_source_batch(part, text):
  with open(f"{SOURCE_PATH_LOCAL}/{part}.csv", 'w') as f:
    f.write(text)

def write_to_memory(df, query_name):
  query = (df
      .writeStream
      .format("memory")
      .queryName(query_name)
      .start()
  )
  return query


def test_read_autoloader_several_batches():
  setup()
  spark = get_spark()

  # start test stream
  streaming_df = read_autoloader(spark, SOURCE_PATH, CHECKPOINT_LOCATION+"/1")
  query = write_to_memory(streaming_df, QUERY_NAME)

  assert spark.table(QUERY_NAME).count() == 0, "Empty streaming DataFrame expected to start"

  write_source_batch("part1", text1)
  time.sleep(30)
  assert spark.table(QUERY_NAME).count() == 2, "Expected 2 records after first batch"

  write_source_batch("part2", text2)
  time.sleep(30)
  assert spark.table(QUERY_NAME).count() == 4, "Expected 4 records after second batch"

  query.stop()

def test_delay_type_valid():
  spark = get_spark()
  streaming_df = read_autoloader(spark, SOURCE_PATH, CHECKPOINT_LOCATION+"/2")
  result_df = delay_type_transform(streaming_df)
  query = write_to_memory(result_df, QUERY_NAME)

  write_source_batch("part1", text1)
  write_source_batch("part2", text2)
  write_source_batch("part3", text3)
  time.sleep(30)
  
  df = spark.table(QUERY_NAME)

  assert df.where(expr("delay_type = 'WeatherDelay'")).count() == 1
  assert df.where(expr("delay_type = 'NASDelay'")).count() == 1

  query.stop()



