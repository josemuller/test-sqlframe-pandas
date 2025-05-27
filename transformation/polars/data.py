import polars as pl

def data(df_func2: pl.DataFrame,
         df_func1: pl.DataFrame,
         df_func3: pl.DataFrame,
         df_func4: pl.DataFrame,
         df_func5: pl.DataFrame) -> pl.DataFrame:

    df = (
        df_func2
        .join(df_func3, on="func2_col4_id", how="inner")
        .join(df_func1, on="func1_id", how="inner")
        .join(df_func4, on="func4_col1_id", how="left")
        .join(df_func5, on="func4_col1_id", how="left")
        .with_columns([
            (pl.col("func2_col7") == 1).cast(pl.Int8).alias("aa"),
            (pl.col("func2_col7") == 2).cast(pl.Int8).alias("bb"),
            (pl.col("func3_col5") == "aa").cast(pl.Int8).alias("cc"),
            (pl.col("func3_col5") == "bb").cast(pl.Int8).alias("dd"),
            (pl.col("func3_col5") == "cc").cast(pl.Int8).alias("ee"),
            (pl.col("func3_col5") == "dd").cast(pl.Int8).alias("ff"),
            (pl.col("func3_col5") == "ee").cast(pl.Int8).alias("gg"),
            pl.col("o_func3_col4").fill_null(-1).alias("hh"),
        ])
    )

    # Replace all remaining nulls with "-1"
    df = df.fill_null("-1")

    # Replace any remaining nulls again and filter rows where func4_col2 can be parsed as integer
    df = (
        df
        .with_columns([
            pl.when(pl.col(col).is_null()).then(pl.lit("-1")).otherwise(pl.col(col)).alias(col)
            for col in df.columns
        ])
        .filter(pl.col("func4_col2").cast(pl.Int32).is_not_null())
    )

    return df
