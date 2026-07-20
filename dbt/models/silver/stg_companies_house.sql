{{ config(materialized='view') }}

SELECT
    TRIM(INITCAP(company_name))              AS company_name,
    company_number,
    company_status,
    company_type,

    SAFE_CAST(date_of_creation AS DATE)      AS date_of_creation,

    sic_codes,

    registered_office_address,

    TIMESTAMP(matched_at)                    AS matched_at,

    UPPER(TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(company_name, r'\s+(LIMITED|LTD|PLC)\.?$', ''),
        r'[^A-Za-z0-9 ]', ''
    ))) AS name_for_matching

FROM {{ source('uk_sponsor_pipeline', 'raw_companies_house') }}
