def prep_func4(df):
    return (
        df
        .withColumnRenamed("func4_col1_id1", "func4_col1_id")
    )


if __name__ == "__main__":
    from flypipe.tests.spark import build_spark
    spark = build_spark()
    df = spark.read.parquet("../raw_data/func4.parquet")
    df.show(truncate=False)
    df = prep_func4(df)
    df.show(truncate=False)