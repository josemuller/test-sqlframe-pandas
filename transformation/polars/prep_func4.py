import polars as pl

def prep_func4(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({"func4_col1_id1": "func4_col1_id"})

if __name__ == "__main__":
    df = pl.read_parquet("../raw_data/func4.parquet")
    print(df)

    df = prep_func4(df)
    print(df)
