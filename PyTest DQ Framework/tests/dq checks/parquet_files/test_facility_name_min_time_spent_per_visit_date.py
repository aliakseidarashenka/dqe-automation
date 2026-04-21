"""
Description: data Quality checks for facility_name_min_time_spent_per_visit_date dataset.
Requirement(s): TICKET-1234
Author(s): Name Surname
"""

import pytest
import os

@pytest.fixture(scope="module")
def target_data(parquet_reader):
    path = os.path.join(
        os.path.dirname(__file__),
        "../../parquet_data/facility_name_min_time_spent_per_visit_date"
    )
    return parquet_reader.process(path)


@pytest.mark.parquet_data
@pytest.mark.smoke
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(target_data)

@pytest.mark.parquet_data
def test_check_not_null_values(target_data, data_quality_library):
    data_quality_library.check_not_null_values(
        target_data,
        column_names=["facility_name", "visit_date", "min_time_spent"])

@pytest.mark.parquet_data
@pytest.mark.xfail(reason="Need to confirm whether facility_name + visit_date must be unique")
def test_check_duplicates(target_data, data_quality_library):
    data_quality_library.check_duplicates(
        target_data,
        column_names=["facility_name", "visit_date", "min_time_spent"]
    )

#@pytest.mark.parquet_data
#def test_check_expected_row_count(target_data, data_quality_library):
#    data_quality_library.check_expected_row_count(target_data, expected_count=2)


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
    data_quality_library.check_count(source_data, target_data)
