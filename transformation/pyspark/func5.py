import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DecimalType

def func5(df):
    df = (
        df
        .withColumn("b", F.get_json_object("func5_col3", "$.B[0]").cast(StringType()))
        .withColumn("c", F.get_json_object("func5_col3", "$.C[0].D[0]").cast(StringType()))
        .withColumn("d", F.get_json_object("func5_col3", "$.D[0]").cast(IntegerType()))
        .withColumn("e", F.get_json_object("func5_col3", "$.E[0]").cast(IntegerType()))
        .withColumn("f", F.get_json_object("func5_col3", "$.F[0]").cast(IntegerType()))
        .withColumn("d", F.coalesce(F.col("d"), F.lit(0)))
        .withColumn("e", F.coalesce(F.col("e"), F.lit(0)))
        .withColumn("f", F.coalesce(F.col("f"), F.lit(0)))
        .filter(
            F.when(
                (F.col("b") == F.lit("A")) & (F.col("c").isin("A", "b", "c")), True
            ).when(
                (F.col("b") == F.lit("B")) & (F.col("c").isin("d", "e")), True
            ).when(
                (F.col("b") == F.lit("C")) & (F.col("c").isin("f", "g", "h", "i")), True
            ).otherwise(False)
        )
        .withColumn("d", F.col("d").cast(DecimalType(13, 2)))
        .withColumn("e", F.col("e").cast(DecimalType(13, 2)))
        .withColumn("f", F.col("f").cast(DecimalType(13, 2)))
        .groupBy("func4_col1_id1")
        .agg(
            F.sum("d").alias("d"),
            F.sum("e").alias("e"),
            F.sum("f").alias("f")
        )
    )

    # new columns
    df = (
        df
        .withColumn("func5_col5", F.when(
            (F.col("d") > 0) & (F.col("f") > 0),
            F.round(F.col("f") / F.col("d"), 2)).otherwise(0)
                    )
        .withColumn("func5_col6", F.when(
            (F.col("d") > 0) & (F.col("e") > 0),
            F.round(F.col("e") / F.col("d"), 2)).otherwise(0)
                    )
    )

    return df.withColumnRenamed("func4_col1_id1", "func4_col1_id")


if __name__ == "__main__":
    from flypipe.tests.spark import build_spark

    spark = build_spark()
    df = spark.read.parquet("../raw_data/func5.parquet")
    df.show(truncate=False)

    df = func5(df)
    df.show(truncate=False)