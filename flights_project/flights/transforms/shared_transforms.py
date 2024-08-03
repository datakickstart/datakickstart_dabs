"""Python functions to test
These represent Python functions that you would keep in a Python file and import to test.
"""

def add_metadata_columns(df, include_time=True):
    if include_time:
        df = df.withColumn("last_updated_time", current_timestamp())
    else:
        df = df.withColumn("last_updated_date", current_date())
     
    df = df.withColumn("source_file", input_file_name())
    return df


def append_to_delta(df, dest_table, streaming=False, checkpoint_location=None):
    if not streaming:
        df.write.format("delta").mode("append").saveAsTable(dest_table)
    else:
        df.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_location).toTable(dest_table)
