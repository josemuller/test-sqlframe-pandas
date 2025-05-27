import pandas as pd
import json
import numpy as np
from decimal import Decimal, ROUND_HALF_UP, DivisionByZero, InvalidOperation


def safe_div(x, y):
    try:
        if x > 0 and y > 0:
            return (y / x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        else:
            return Decimal("0.00")
    except (DivisionByZero, InvalidOperation):
        return Decimal("0.00")

def func5(df):
    # Parse JSON content into new columns
    df["parsed_json"] = df["func5_col3"].apply(lambda x: json.loads(x.replace("'", '"')))

    df["b"] = df["parsed_json"].apply(lambda x: x.get("B", [None])[0])
    df["c"] = df["parsed_json"].apply(lambda x: x.get("C", [{}])[0].get("D", [None])[0])
    df["d"] = df["parsed_json"].apply(lambda x: x.get("D", [None])[0])
    df["e"] = df["parsed_json"].apply(lambda x: x.get("E", [None])[0])
    df["f"] = df["parsed_json"].apply(lambda x: x.get("F", [None])[0])

    # Replace None with 0
    df["d"] = df["d"].fillna(0).astype(float)
    df["e"] = df["e"].fillna(0).astype(float)
    df["f"] = df["f"].fillna(0).astype(float)

    # Apply filter logic
    mask = (
        ((df["b"] == "A") & (df["c"].isin(["A", "b", "c"]))) |
        ((df["b"] == "B") & (df["c"].isin(["d", "e"]))) |
        ((df["b"] == "C") & (df["c"].isin(["f", "g", "h", "i"])))
    )
    df = df[mask]

    # Cast to Decimal with precision 13, 2
    def to_decimal(val):
        return Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    df["d"] = df["d"].apply(to_decimal)
    df["e"] = df["e"].apply(to_decimal)
    df["f"] = df["f"].apply(to_decimal)

    # Group and aggregate
    agg_df = df.groupby("func4_col1_id1", as_index=False).agg({
        "d": "sum",
        "e": "sum",
        "f": "sum"
    })

    # New calculated columns
    agg_df["func5_col5"] = agg_df.apply(lambda row: safe_div(row["d"], row["f"]), axis=1)
    agg_df["func5_col6"] = agg_df.apply(lambda row: safe_div(row["d"], row["e"]), axis=1)

    return agg_df.rename(columns={"func4_col1_id1": "func4_col1_id"})


if __name__ == "__main__":
    import pandas as pd
    from tabulate import tabulate

    df = pd.read_parquet("../raw_data/func5.parquet")
    print(tabulate(df, headers=df.columns))

    df = func5(df)
    print(tabulate(df, headers=df.columns))