*** Settings ***
Library    SeleniumLibrary
Library    helper.py

*** Variables ***
${REPORT_FILE}       data/report.html
${PARQUET_FOLDER}    data/parquet_data/facility_type_avg_time_spent_per_visit_date

*** Test Cases ***
Compare HTML Report With Parquet Dataset
    Open Browser    file://${CURDIR}/${REPORT_FILE}    Chrome
    Wait Until Page Contains    HTML Report    timeout=10s

    ${html_df}=       Read Html Table       ${REPORT_FILE}
    ${parquet_df}=    Read Parquet Dataset  ${PARQUET_FOLDER}

    ${result}    ${message}=    Compare Dataframes    ${html_df}    ${parquet_df}

    Should Be True    ${result}    ${message}

    Close Browser