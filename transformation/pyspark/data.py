import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

def data(df_func2, df_func1, df_func3, df_func4, df_func5):
    df = (
        df_func2
        .join(df_func3, on="func2_col4_id", how="inner")
        .join(df_func1, on="func1_id", how="inner")
        .join(df_func4, on="func4_col1_id", how="left")
        .join(df_func5, on="func4_col1_id", how="left")
        .withColumn("aa", F.when(F.col("func2_col7") == 1, 1).otherwise(0))
        .withColumn("bb", F.when(F.col("func2_col7") == 2, 1).otherwise(0))
        .withColumn("cc", F.when(F.col("func3_col5") == "aa", 1).otherwise(0))
        .withColumn("dd", F.when(F.col("func3_col5") == "bb", 1).otherwise(0))
        .withColumn("ee", F.when(F.col("func3_col5") == "cc", 1).otherwise(0))
        .withColumn("ff", F.when(F.col("func3_col5") == "dd", 1).otherwise(0))
        .withColumn("gg", F.when(F.col("func3_col5") == "ee", 1).otherwise(0))
        .withColumn("hh", F.coalesce(F.col("o_func3_col4"), F.lit(-1)))
    )

    df = df.fillna("-1")

    df = (
        df
        .select([F.coalesce(F.col(col), F.lit("-1")).alias(col) for col in df.columns])
        .filter(F.col("func4_col2").cast(IntegerType()).isNotNull())
    )

    return df