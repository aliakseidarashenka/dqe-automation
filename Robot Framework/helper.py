import json
import re

import pandas as pd

# func for reading html plotly table
def read_html_table(html_path) -> pd.DataFrame:
    with open(html_path, "r", encoding="utf-8") as file:
        html = file.read()

    match = re.search(
        r'Plotly\.newPlot\(\s*"[^"]+"\s*,\s*(\[.*?\])\s*,\s*\{',
        html,
        re.DOTALL,
    )

    if not match:
        raise ValueError("Plotly data was not found in HTML report")

    plotly_data = json.loads(match.group(1))

    table_data = None
    for item in plotly_data:
        if item.get("type") == "table":
            table_data = item
            break

    if table_data is None:
        raise ValueError("Plotly table was not found in HTML report")

    headers = table_data["header"]["values"]
    values = table_data["cells"]["values"]

    df = pd.DataFrame(dict(zip(headers, values)))

    df = df.rename(columns={
        "Facility Type": "facility_type",
        "Visit Date": "visit_date",
        "Average Time Spent": "avg_time_spent",
    })

    return df

# func for reading parquet by the link
def read_parquet_dataset(parquet_folder, filter_date: str | None = None) -> pd.DataFrame:
    df = pd.read_parquet(parquet_folder)

    if filter_date:
        df = df[df["visit_date"].astype(str) == filter_date]

    df = df[["facility_type", "visit_date", "avg_time_spent"]]

    df["visit_date"] = df["visit_date"].astype(str)
    df["avg_time_spent"] = df["avg_time_spent"].astype(float)

    return df

# func for compare two dfs
def compare_dataframes(html_df: pd.DataFrame, parquet_df: pd.DataFrame):
    html_df = html_df.copy()
    parquet_df = parquet_df.copy()

    html_df["visit_date"] = html_df["visit_date"].astype(str)
    parquet_df["visit_date"] = parquet_df["visit_date"].astype(str)

    dates = html_df["visit_date"].unique()
    parquet_df = parquet_df[parquet_df["visit_date"].isin(dates)]

    html_df["avg_time_spent"] = html_df["avg_time_spent"].astype(float).round(2)
    parquet_df["avg_time_spent"] = parquet_df["avg_time_spent"].astype(float).round(2)

    columns = ["facility_type", "visit_date", "avg_time_spent"]

    html_df = html_df[columns].sort_values(columns).reset_index(drop=True)
    parquet_df = parquet_df[columns].sort_values(columns).reset_index(drop=True)

    if html_df.equals(parquet_df):
        return True, "Match"

    diff = html_df.compare(parquet_df, keep_shape=True, keep_equal=False)
    return False, diff.to_string()