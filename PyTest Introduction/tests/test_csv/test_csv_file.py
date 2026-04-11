import pytest
import re


def test_file_not_empty(csv_data):
    assert not csv_data.empty, "CSV file is empty"


@pytest.mark.validate_csv
def test_schema(csv_data, validate_schema):
    expected = ["id", "name", "age", "email", "is_active"]
    validate_schema(csv_data.columns, expected)


@pytest.mark.validate_csv
@pytest.mark.skip(reason="Not implemented yet")
def test_age_range(csv_data):
    assert csv_data["age"].between(0, 100).all(), \
        "Age values are out of range"


@pytest.mark.validate_csv
def test_email_format(csv_data):
    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"

    for email in csv_data["email"]:
        assert re.match(pattern, email), \
            f"Invalid email format: {email}"


@pytest.mark.validate_csv
@pytest.mark.xfail(reason="Known issue with duplicates")
def test_no_duplicates(csv_data):
    assert not csv_data.duplicated().any(), \
        "Duplicate rows found"


@pytest.mark.parametrize("user_id, expected", [
    (1, False),
    (2, True),
])
def test_is_active(csv_data, user_id, expected):
    value = csv_data.loc[csv_data["id"] == user_id, "is_active"].iloc[0]
    assert value == expected, \
        f"is_active for id={user_id} should be {expected}"


def test_is_active_id_2(csv_data):
    value = csv_data.loc[csv_data["id"] == 2, "is_active"].iloc[0]
    assert value is True, "id=2 should be active"