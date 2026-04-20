import pandas as pd

df = pd.DataFrame({
    "name": ["A", "B"],
    "visit_date": ["2026-01-01", "2026-01-02"],
    "min_time_spent": [10, 20]
})

df.to_parquet("test.parquet")