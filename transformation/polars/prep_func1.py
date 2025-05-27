import polars as pl

def prep_func1(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_columns([
            pl.col("func1_col2").cast(pl.Decimal(13, 2)),
            pl.col("func1_col3").cast(pl.Decimal(13, 2)),
            pl.col("func1_col5").cast(pl.Int32),
            pl.col("func1_col4").cast(pl.Int32)
        ])
        .with_columns([
            pl.when(pl.col("func1_col2").is_not_null())
              .then(pl.col("func1_col2"))
              .otherwise(pl.col("func1_col3") * 12)
              .alias("func1_col2"),

            (pl.col("func1_col5") + 12 * pl.col("func1_col4")).alias("func1_col5")
        ])
    )

if __name__ == "__main__":
    # Example read (adjust path and file format as needed)
    df = pl.read_parquet("../raw_data/func1.parquet")
    print(df)

    df = prep_func1(df)
    print(df)
