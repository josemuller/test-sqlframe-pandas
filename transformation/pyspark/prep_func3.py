import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType

def prep_func3(df_new_loans):
    return (
        df_new_loans
        .withColumn("func3_col5", F.when(F.col("func3_col5").isin("A", "B", "C"), "D").otherwise(F.col("func3_col5")))
        .withColumn("func3_col5", F.when(F.col("func3_col5").isin("E", "F"), "G").otherwise(F.col("func3_col5")))
        .withColumn("func3_col1", F.col("func3_col1").cast(DecimalType(13,2)))

        # New Columns
        .withColumn("o_func3_col1", F.when(F.col("func3_col5")=="H", F.col("func3_col1")/100).otherwise(F.col("func3_col1")))
        .withColumn("o_func3_col4", F.when(F.col("func3_col5")=="H", F.col("func3_col6")).otherwise(F.col("func3_col4")))
    )


if __name__ == "__main__":
    from flypipe.tests.spark import build_spark
    spark = build_spark()
    df = spark.read.parquet("../raw_data/func3.parquet")
    df.show(truncate=False)

    df = prep_func3(df)
    df.show(truncate=False)