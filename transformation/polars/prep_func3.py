import polars as pl

def prep_func3(df_new_loans: pl.DataFrame) -> pl.DataFrame:
    return (
        df_new_loans
        .with_columns([
            # Step 1: Normalize func3_col5 using two-step conditional replacements
            pl.when(pl.col("func3_col5").is_in(["A", "B", "C"]))
              .then(pl.lit("D"))
              .when(pl.col("func3_col5").is_in(["E", "F"]))
              .then(pl.lit("G"))
              .otherwise(pl.col("func3_col5"))
              .alias("func3_col5"),

            # Step 2: Cast func3_col1 to Decimal (or use Float64 if Decimal unsupported)
            pl.col("func3_col1").cast(pl.Decimal(13, 2)).alias("func3_col1")
        ])
        .with_columns([
            # Step 3: Derived columns
            pl.when(pl.col("func3_col5") == "H")
              .then(pl.col("func3_col1") / 100)
              .otherwise(pl.col("func3_col1"))
              .alias("o_func3_col1"),

            pl.when(pl.col("func3_col5") == "H")
              .then(pl.col("func3_col6"))
              .otherwise(pl.col("func3_col4"))
              .alias("o_func3_col4")
        ])
    )

if __name__ == "__main__":
    df = pl.read_parquet("../raw_data/func3.parquet")
    print(df)

    df = prep_func3(df)
    print(df)
