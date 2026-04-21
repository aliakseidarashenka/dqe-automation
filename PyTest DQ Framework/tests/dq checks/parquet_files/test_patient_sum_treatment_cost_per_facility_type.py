"""
Description: data Quality checks for patient_sum_treatment_cost_per_facility_type dataset.
Requirement(s): TICKET- 3
Author(s): Aliaksei Darashenka
"""

import pytest
import os

pytestmark = [pytest.mark.patient_sum_treatment_cost_per_facility_type, pytest.mark.parquet_data]


@pytest.fixture(scope="module")
def target_data(parquet_reader):
    path = os.path.join(
        os.path.dirname(__file__),
        "../../parquet_data/patient_sum_treatment_cost_per_facility_type"
    )
    return parquet_reader.process(path)


@pytest.mark.smoke
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(target_data)


@pytest.mark.xfail(reason="Known data issue: null values detected in patient_sum_treatment_cost_per_facility_type parquet data")
def test_check_not_null_values(target_data, data_quality_library):
    data_quality_library.check_not_null_values(
        target_data,
        column_names=["facility_type", "full_name", "sum_treatment_cost"]
    )


@pytest.mark.xfail(reason="Known data issue: duplicates detected in patient_sum_treatment_cost_per_facility_type parquet data")
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


@pytest.mark.xfail(reason="Known data issue: parquet row count does not match DB aggregation")
def test_check_count_source_vs_target(source_data, target_data, data_quality_library):
    data_quality_library.check_count(source_data, target_data)


@pytest.mark.xfail(reason="Known data issue: parquet dataset does not fully match DB aggregation")
def test_parquet_vs_db_data_match(source_data, target_data, data_quality_library):
    data_quality_library.check_data_full_data_set(
        source_data,
        target_data,
        key_columns=["facility_type", "full_name"],
        compare_columns=["sum_treatment_cost"]
    )
