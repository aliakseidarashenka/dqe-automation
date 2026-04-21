"""
Description: data Quality checks for facility_name_min_time_spent_per_visit_date dataset.
Requirement(s): TICKET- 1
Author(s): Aliaksei Darashenka
"""

import pytest
import os

pytestmark = [pytest.mark.facility_name_min_time_spent_per_visit_date, pytest.mark.parquet_data]

@pytest.fixture(scope="module")
def target_data(parquet_reader):
    """
        Load parquet dataset 
    """
    return parquet_reader.read("facility_name_min_time_spent_per_visit_date")


# BASIC DQ CHECKS

def test_check_dataset_is_not_empty(target_data, data_quality_library):
    """
        Check that parquet is not empty
    """
    data_quality_library.check_dataset_is_not_empty(target_data)

def test_check_not_null_values(target_data, data_quality_library):
    """
        Check that parquet don't contatin null values in the list of column
    """
    data_quality_library.check_not_null_values(
        target_data,
        column_names=["facility_name", "visit_date", "min_time_spent"])

# @pytest.mark.xfail(reason="Need to confirm whether facility_name + visit_date must be unique")
def test_check_duplicates(target_data, data_quality_library):
    """
        Check uniqness by composite key
    """
    data_quality_library.check_duplicates(
        target_data,
        column_names=["facility_name", "visit_date", "min_time_spent"]
    )

# Business logic check
@pytest.fixture(scope="module")
def source_data(db_connection):
    query = """
        select
            f.facility_name,
            cast(v.visit_timestamp as date) as visit_date,
            min(v.duration_minutes) as min_time_spent
        from public.facilities f
        left join public.visits v
            on v.facility_id = f.id
        group by
            f.facility_name,
            cast(v.visit_timestamp as date)
    """
    return db_connection.get_data_sql(query)

@pytest.mark.parquet_data
def test_check_count_source_vs_target(source_data, target_data, data_quality_library):
    """
        Row count in parquet and DB should match
    """
    data_quality_library.check_count(source_data, target_data)


def test_parquet_vs_db_data_match(
    target_data,
    db_connection,
    data_quality_library
):
    """Parquet data should fully match DB aggregation"""

    source_data = get_avg_visit_data(db_connection)

    data_quality_library.check_data_full_data_set(
        target_data,
        source_data,
        key_columns=["facility_type", "visit_date"],
        compare_columns=["min_time_spent"]
    )


