import pandas as pd
import numpy as np

def data(df_func2, df_func1, df_func3, df_func4, df_func5):
    # Perform joins
    df = df_func2.merge(df_func3, on="func2_col4_id", how="inner") \
                 .merge(df_func1, on="func1_id", how="inner") \
                 .merge(df_func4, on="func4_col1_id", how="left") \
                 .merge(df_func5, on="func4_col1_id", how="left")

    # Add calculated columns
    df["aa"] = np.where(df["func2_col7"] == 1, 1, 0)
    df["bb"] = np.where(df["func2_col7"] == 2, 1, 0)
    df["cc"] = np.where(df["func3_col5"] == "aa", 1, 0)
    df["dd"] = np.where(df["func3_col5"] == "bb", 1, 0)
    df["ee"] = np.where(df["func3_col5"] == "cc", 1, 0)
    df["ff"] = np.where(df["func3_col5"] == "dd", 1, 0)
    df["gg"] = np.where(df["func3_col5"] == "ee", 1, 0)

    # Coalesce column 'o_func3_col4' with fallback to -1
    df["hh"] = df["o_func3_col4"].fillna(-1)

    # Replace all NaNs in the DataFrame with "-1"
    df = df.fillna("-1")

    # Coalesce each column individually again (optional if fillna already covers it)
    df = df.map(lambda x: "-1" if pd.isna(x) else x)

    # Filter rows where 'func4_col2' is numeric (not null after cast to integer)
    df = df[pd.to_numeric(df["func4_col2"], errors='coerce').notnull()]

    return df
