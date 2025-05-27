def prep_func4(df):
    return df.rename(columns={"func4_col1_id1": "func4_col1_id"})

if __name__ == "__main__":
    import pandas as pd
    from tabulate import tabulate

    df = pd.read_parquet("../raw_data/func4.parquet")
    print(tabulate(df, headers=df.columns))

    df = prep_func4(df)
    print(tabulate(df, headers=df.columns))