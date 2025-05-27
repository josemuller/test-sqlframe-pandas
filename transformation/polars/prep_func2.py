import polars as pl

def prep_func2(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df
        .with_columns([
            pl.col("func2_col1").str.json_path_match("$.func2_col1_number").alias("func2_col1_number"),
            pl.col("func2_col1").str.json_path_match("$.func2_col1_id").alias("func2_col1_id"),
            pl.when(pl.col("func2_col3") == "A").then(pl.lit("P"))
              .when(pl.col("func2_col3") == "X").then(pl.lit("Z"))
          .otherwise(pl.col("func2_col3"))
          .alias("func2_col3")
        ])
        .filter(pl.col("func2_col1_id").is_not_null())
        .filter(pl.col("func2_col6") > pl.lit("2024-01-01").cast(pl.Date))
        .with_columns([
            pl.col("func2_col2").str.contains("(?i)xyz").cast(pl.Int8).alias("func2_col7"),
            pl.when((pl.col("func2_col3") == "P") & (pl.col("func2_col2").str.contains("(?i)xyz")))
              .then(pl.lit("Z"))
              .otherwise(pl.col("func2_col3"))
              .alias("func2_col8")
        ])
        .rename({"func4_col1_id1": "func4_col1_id"})
    )

    return df

if __name__ == "__main__":
    df = pl.read_parquet("../raw_data/func2.parquet")
    print(df)

    df = prep_func2(df)
    print(df)
