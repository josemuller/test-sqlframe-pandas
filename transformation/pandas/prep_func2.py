import json
import pandas as pd
import numpy as np


def extract_json_field(json_str, field_path):
    try:
        data = json.loads(json_str)
        for key in field_path:
            if isinstance(key, str):
                data = data.get(key)
            elif isinstance(key, int) and isinstance(data, list):
                data = data[key] if key < len(data) else None
            else:
                return None
        return data
    except (json.JSONDecodeError, TypeError, AttributeError):
        return None

def prep_func2(df):
    # Extract fields from JSON
    df["func2_col1_number"] = df["func2_col1"].apply(lambda x: extract_json_field(x, ["func2_col1_number"]))
    df["func2_col1_id"] = df["func2_col1"].apply(lambda x: extract_json_field(x, ["func2_col1_id"]))

    # Filter: func2_col1_id not null and func2_col6 > 2024-01-01
    df = df[df["func2_col1_id"].notnull()]
    df["func2_col6"] = pd.to_datetime(df["func2_col6"], errors='coerce')
    df = df[df["func2_col6"] > pd.Timestamp("2024-01-01")]

    # Normalize func2_col3
    df["func2_col3"] = df["func2_col3"].replace({
        "A": "P",
        "X": "Z"
    })

    # New columns
    # Boolean for consolidation
    df["func2_col7"] = df["func2_col2"].fillna("").str.contains("xyz", case=False, na=False).astype(int)

      # Conditional func2_col8
    df["func2_col8"] = np.where(
        (df["func2_col3"] == "P") & (df["func2_col7"] == 1),
        "Z",
        df["func2_col3"]
    )
    return df.rename(columns={"func4_col1_id1": "func4_col1_id"})

if __name__ == "__main__":
    import pandas as pd
    from tabulate import tabulate

    df = pd.read_parquet("../raw_data/func2.parquet")
    print(tabulate(df, headers=df.columns))

    df = prep_func2(df)
    print(tabulate(df, headers=df.columns))