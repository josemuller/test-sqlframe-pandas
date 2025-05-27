import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DecimalType


def prep_func1(df):
    return (
        df
        .withColumn("func1_col2", F.col("func1_col2").cast(DecimalType(13, 2)))
        .withColumn("func1_col3", F.col("func1_col3").cast(DecimalType(13, 2)))
        .withColumn("func1_col5", F.col("func1_col5").cast(IntegerType()))
        .withColumn("func1_col4", F.col("func1_col4").cast(IntegerType()))
        .withColumn(
            "func1_col2",
            F.when(F.col("func1_col2").isNotNull(), F.col("func1_col2")).otherwise(F.col("func1_col3") * 12),
        )
        .withColumn("func1_col5", F.col("func1_col5") + (12 * F.col("func1_col4")))
    )


if __name__ == "__main__":
    from flypipe.tests.spark import build_spark

    spark = build_spark()
    df = spark.read.parquet("../raw_data/func1.parquet")
    df.show(truncate=False)
    df = prep_func1(df)
    df.show(truncate=False)