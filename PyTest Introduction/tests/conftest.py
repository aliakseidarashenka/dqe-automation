import pytest
import pandas as pd
from pathlib import Path


# Fixture to read the CSV file
@pytest.fixture(scope="session")
def csv_data():
    file_path = Path(__file__).parent.parent / "src" / "data" / "data.csv"
    return pd.read_csv(file_path)

# Fixture to validate the schema of the file
@pytest.fixture(scope="session")
def validate_schema():
    def _validate(actual_schema, expected_schema):
        assert list(actual_schema) == expected_schema, (
            f"Schema mismatch. Expected {expected_schema}, got {list(actual_schema)}"
        )
    return _validate


# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(config, items):
    for item in items:
        if not item.own_markers:
            item.add_marker(pytest.mark.unmarked)