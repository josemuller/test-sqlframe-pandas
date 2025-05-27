import pandas as pd

def prep_func1(df):
    # Convert relevant columns to numeric (similar to DecimalType/IntegerType casting)
    df["func1_col2"] = pd.to_numeric(df["func1_col2"], errors='coerce')
    df["func1_col3"] = pd.to_numeric(df["func1_col3"], errors='coerce')
    df["func1_col5"] = pd.to_numeric(df["func1_col5"], errors='coerce').fillna(0).astype(int)
    df["func1_col4"] = pd.to_numeric(df["func1_col4"], errors='coerce').fillna(0).astype(int)

    # Compute derived columns
    df["func1_col2"] = df["func1_col2"].combine_first(df["func1_col3"] * 12)
    df["func1_col5"] = df["func1_col5"] + 12 * df["func1_col4"]

    return df

if __name__ == "__main__":
    import pandas as pd
    from tabulate import tabulate

    df = pd.read_parquet("../raw_data/func1.parquet")
    print(tabulate(df, headers=df.columns))

    df = prep_func1(pd.read_parquet("../raw_data/func1.parquet"))
    print(tabulate(df, headers=df.columns))