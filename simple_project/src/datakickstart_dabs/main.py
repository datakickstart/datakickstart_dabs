def spark_session():
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()   
    except (ValueError, RuntimeError):
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.profile("unit_tests").getOrCreate()    
    except ImportError:
        print("No Databricks Connect, build and return local SparkSession")
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

def get_taxis():
  spark = spark_session()
  return spark.read.table("samples.nyctaxi.trips")

def main():
  get_taxis().show(5)

if __name__ == '__main__':
  main()
  print("Completed main job")