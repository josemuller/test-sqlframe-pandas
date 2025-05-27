import numpy as np
import pandas as pd

def prep_func3(df):

    # Normalize loan type
    df.loc[df["func3_col5"].isin(["A", "B", "C"]), "func3_col5"] = "D"
    df.loc[df["func3_col5"].isin(["E", "F"]), "func3_col5"] = "G"

    # Cast apr to decimal (float64 in pandas)
    df["func3_col1"] = pd.to_numeric(df["func3_col1"], errors='coerce')

    # Compute offer_apr
    df["o_func3_col1"] = np.where(
        df["func3_col5"] == "H",
        df["func3_col1"] / 100,
        df["func3_col1"]
    )

    # Compute offer_balance
    df["o_func3_col4"] = np.where(
        df["func3_col5"] == "H",
        df["func3_col6"],
        df["func3_col4"]
    )

    return df


if __name__ == "__main__":
    import pandas as pd
    from tabulate import tabulate

    df = pd.read_parquet("../raw_data/func3.parquet")
    print(tabulate(df, headers=df.columns))

    df = prep_func3(df)
    print(tabulate(df, headers=df.columns))