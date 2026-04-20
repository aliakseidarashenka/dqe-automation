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
    def check_data_full_data_set(df1, df2):
        assert df1.equals(df2), "DataFrames are not equal"

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

