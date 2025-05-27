import polars as pl

def func5(df: pl.DataFrame) -> pl.DataFrame:
    # Extract and cast JSON fields
    df = df.with_columns([
        pl.col("func5_col3").str.json_path_match("$.B[0]").cast(pl.String).alias("b"),
        pl.col("func5_col3").str.json_path_match("$.C[0].D[0]").cast(pl.String).alias("c"),
        pl.col("func5_col3").str.json_path_match("$.D[0]").cast(pl.Int32).alias("d"),
        pl.col("func5_col3").str.json_path_match("$.E[0]").cast(pl.Int32).alias("e"),
        pl.col("func5_col3").str.json_path_match("$.F[0]").cast(pl.Int32).alias("f"),
    ])

    # Coalesce d/e/f with 0 if null
    df = df.with_columns([
        pl.col("d").fill_null(0),
        pl.col("e").fill_null(0),
        pl.col("f").fill_null(0)
    ])

    # Apply filtering logic
    df = df.filter(
        ((pl.col("b") == "A") & pl.col("c").is_in(["A", "b", "c"])) |
        ((pl.col("b") == "B") & pl.col("c").is_in(["d", "e"])) |
        ((pl.col("b") == "C") & pl.col("c").is_in(["f", "g", "h", "i"]))
    )

    # Cast to decimal (use Float64 if Decimal not needed)
    df = df.with_columns([
        pl.col("d").cast(pl.Decimal(13, 2)),
        pl.col("e").cast(pl.Decimal(13, 2)),
        pl.col("f").cast(pl.Decimal(13, 2)),
    ])

    # Group by and aggregate
    df = df.group_by("func4_col1_id1").agg([
        pl.sum("d").alias("d"),
        pl.sum("e").alias("e"),
        pl.sum("f").alias("f"),
    ])

    # New computed columns with rounding
    df = df.with_columns([
        pl.when((pl.col("d") > 0) & (pl.col("f") > 0))
          .then((pl.col("f") / pl.col("d")).round(2))
          .otherwise(0)
          .alias("func5_col5"),

        pl.when((pl.col("d") > 0) & (pl.col("e") > 0))
          .then((pl.col("e") / pl.col("d")).round(2))
          .otherwise(0)
          .alias("func5_col6")
    ])

    # Rename column
    return df.rename({"func4_col1_id1": "func4_col1_id"})

if __name__ == "__main__":
    df = pl.read_parquet("../raw_data/func5.parquet")
    print(df)

    df = func5(df)
    print(df)
