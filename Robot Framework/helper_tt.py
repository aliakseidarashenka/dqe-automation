from helper import read_html_table, read_parquet_dataset, compare_dataframes

html_df = read_html_table("data/report.html")

parquet_df = read_parquet_dataset(
    "data/parquet_data/facility_type_avg_time_spent_per_visit_date"
)

result, message = compare_dataframes(html_df, parquet_df)

print(result)
print(message)