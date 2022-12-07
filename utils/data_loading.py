def load_dataset_with_spark(spark, schema, path):
    return spark.read.format("csv") \
        .option("header", True) \
        .option("encoding", "utf-8") \
        .schema(schema) \
        .load(path)
