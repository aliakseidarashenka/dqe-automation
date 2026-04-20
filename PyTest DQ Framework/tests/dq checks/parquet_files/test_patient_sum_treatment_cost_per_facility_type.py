"""
Description: data Quality checks for patient_sum_treatment_cost_per_facility_type dataset.
Requirement(s): TICKET-1234
Author(s): Name Surname
"""

import pytest
import os

@pytest.fixture(scope="module")
def target_data(parquet_reader):
    path = os.path.join(
        os.path.dirname(__file__),
        "../../parquet_data/patient_sum_treatment_cost_per_facility_type"
    )
    return parquet_reader.process(path)


@pytest.mark.parquet_data
@pytest.mark.smoke
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(target_data)

@pytest.mark.parquet_data
@pytest.mark.xfail(reason="Need to confirm is null should be here: 4 null detected.")
def test_check_not_null_values(target_data, data_quality_library):
    data_quality_library.check_not_null_values(
        target_data,
        column_names=["facility_type", "full_name", "sum_treatment_cost"])

@pytest.mark.parquet_data
@pytest.mark.xfail(reason="Need to confirm whether facility_name + visit_date must be unique")
def test_check_duplicates(target_data, data_quality_library):
    data_quality_library.check_duplicates(
        target_data,
        column_names=["facility_type", "full_name", "sum_treatment_cost"]
    )


@pytest.fixture(scope="module")
def source_data(db_connection):
    query = """
        SELECT
            f.facility_type,
            p.first_name || ' ' || p.last_name AS full_name,
            ROUND(SUM(v.treatment_cost), 2) AS sum_treatment_cost,
            f.facility_type as facility_type_partition
        FROM visits v
            JOIN facilities f ON v.facility_id = f.id
            JOIN patients p ON v.patient_id = p.id
        GROUP BY
            f.facility_type,
            p.first_name || ' ' || p.last_name
    """
    return db_connection.get_data_sql(query)


@pytest.mark.parquet_data
def test_check_count_source_vs_target(source_data, target_data, data_quality_library):
    data_quality_library.check_count(source_data, target_data)


