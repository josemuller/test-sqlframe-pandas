from datetime import datetime

import pyspark.sql.functions as F

def prep_func2(df):

    df = (
        df
        .withColumn("func2_col1_number",
                    F.get_json_object("func2_col1", "$.func2_col1_number"))
        .withColumn("func2_col1_id",
                    F.get_json_object("func2_col1", "$.func2_col1_id"))
        .withColumn("func2_col3", F.when(F.col("func2_col3")=="A", "P").otherwise(F.when(F.col("func2_col3")=="X", "Z").otherwise(F.col("func2_col3"))))
        .filter(F.col("func2_col1_id").isNotNull()) # We're adding this because we'll join by this with los data
        .filter(F.col("func2_col6")>F.lit(datetime(2024, 1, 1)))

        # New columns
        .withColumn("func2_col7", F.when(F.col("func2_col2").rlike("(?i)xyz"), 1).otherwise(0))
        .withColumn("func2_col8", F.when((F.col("func2_col3")=="P") & (F.col("func2_col7")==1), "Z").otherwise(F.col("func2_col3")))
        .withColumnRenamed("func4_col1_id1", "func4_col1_id")
    )

    return df


if __name__ == "__main__":
    from flypipe.tests.spark import build_spark
    spark = build_spark()
    df = spark.read.parquet("../raw_data/func2.parquet")
    df.show(truncate=False)

    df = prep_func2(df)
    df.show(truncate=False)