import pytest
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual 

from flights.transforms import flight_transforms

@pytest.fixture(scope="module")
def spark_session():
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.profile("unit_tests").getOrCreate()
    except ValueError:
        print("Profile `unit_tests` not found, trying default SparkSession getOrCreate")
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    except ImportError:
        print("No Databricks Connect, build and return local SparkSession")
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


def test_get_flight_schema__valid():
    schema = flight_transforms.get_flight_schema()
    assert schema is not None
    assert len(schema) == 31

def test_delay_type_transform(spark_session):
    input_df = spark_session.createDataFrame([
        ["0","NA","NA","NA", "NO", "NO"],
        ["NA","0","NA","NA", "NO", "NO"],
        ["NA","NA","0","NA", "NO", "NO"],
        ["NA","NA","NA", "0", "NO", "NO"],
        ["NA","NA","NA","NA", "YES", "NO"],
        ["NA","NA","NA","NA", "NO", "YES"],
        ["0","0","0","0", "YES", "YES"],
    ], ["WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay", "IsArrDelayed", "IsDepDelayed"])

    expected_data = [
        ['WeatherDelay'], 
        ['NASDelay'], 
        ['SecurityDelay'], 
        ['LateAircraftDelay'],
        ['UncategorizedDelay'],
        ['UncategorizedDelay'],
        ['WeatherDelay']
    ]

    expected_df = spark_session.createDataFrame(expected_data,["delay_type"])

    result_df = flight_transforms.delay_type_transform(input_df)

    assertDataFrameEqual(result_df.select("delay_type"), expected_df)
    

