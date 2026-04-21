import pandas as pd


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    This class is intended to be used in a PyTest-based testing framework to validate
    the quality of data in DataFrames. Each method performs a specific data quality
    check and uses assertions to ensure that the data meets the expected conditions.
    """

    @staticmethod
    def check_duplicates(df, column_names=None):
        if column_names:
            duplicates_count = df.duplicated(subset=column_names).sum()
        else:
            duplicates_count = df.duplicated().sum()

        assert duplicates_count == 0, f"Found {duplicates_count} duplicate rows"

    @staticmethod
    def check_count(df1, df2):
        assert len(df1) == len(df2), (
            f"Row count mismatch: df1={len(df1)}, df2={len(df2)}"
        )

    @staticmethod
    def check_data_full_data_set(df1, df2, key_columns=None, compare_columns=None):
        if key_columns is None:
            key_columns = []

        if compare_columns is None:
            compare_columns = [
                col for col in df1.columns
                if col in df2.columns and col not in key_columns
            ]

        required_columns = key_columns + compare_columns

        missing_in_df1 = [col for col in required_columns if col not in df1.columns]
        missing_in_df2 = [col for col in required_columns if col not in df2.columns]

        assert not missing_in_df1, f"Missing columns in df1: {missing_in_df1}"
        assert not missing_in_df2, f"Missing columns in df2: {missing_in_df2}"

        df1_prepared = df1[required_columns].copy().sort_values(by=key_columns).reset_index(drop=True)
        df2_prepared = df2[required_columns].copy().sort_values(by=key_columns).reset_index(drop=True)

        assert len(df1_prepared) == len(df2_prepared), (
            f"Row count mismatch before comparison: df1={len(df1_prepared)}, df2={len(df2_prepared)}"
        )

        merged = df1_prepared.merge(
            df2_prepared,
            on=key_columns,
            how="outer",
            suffixes=("_df1", "_df2"),
            indicator=True
        )

        missing_in_df2_rows = merged[merged["_merge"] == "left_only"]
        missing_in_df1_rows = merged[merged["_merge"] == "right_only"]

        assert missing_in_df2_rows.empty, (
            f"Rows exist in df1 but not in df2: {missing_in_df2_rows.head(10).to_dict(orient='records')}"
        )
        assert missing_in_df1_rows.empty, (
            f"Rows exist in df2 but not in df1: {missing_in_df1_rows.head(10).to_dict(orient='records')}"
        )

        for col in compare_columns:
            mismatched = merged[merged[f"{col}_df1"] != merged[f"{col}_df2"]]

            assert mismatched.empty, (
                f"Mismatched values found in column '{col}': "
                f"{mismatched.head(10).to_dict(orient='records')}"
            )

    @staticmethod
    def check_dataset_is_not_empty(df):
        assert not df.empty, "Dataset is empty"

    @staticmethod
    def check_not_null_values(df, column_names=None):
        if column_names is None:
            column_names = df.columns

        for col in column_names:
            null_count = df[col].isnull().sum()
            assert null_count == 0, f"Column '{col}' has {null_count} null values"

    @staticmethod
    def check_expected_row_count(df, expected_count):
        actual_count = len(df)
        assert actual_count == expected_count, (
            f"Row count mismatch: expected={expected_count}, actual={actual_count}"
        )
