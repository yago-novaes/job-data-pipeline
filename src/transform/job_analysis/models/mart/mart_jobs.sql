{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ ref('stg_jobs') }}
),

salary_annualized AS (
    SELECT
        *,
        CASE salary_period
            WHEN 'HOUR'  THEN min_salary * 2080
            WHEN 'MONTH' THEN min_salary * 12
            WHEN 'WEEK'  THEN min_salary * 52
            WHEN 'YEAR'  THEN min_salary
            ELSE NULL
        END AS annual_min_salary,

        CASE salary_period
            WHEN 'HOUR'  THEN max_salary * 2080
            WHEN 'MONTH' THEN max_salary * 12
            WHEN 'WEEK'  THEN max_salary * 52
            WHEN 'YEAR'  THEN max_salary
            ELSE NULL
        END AS annual_max_salary
    FROM source
),

enriched AS (
    SELECT
        job_id,
        job_title,
        company_name,
        is_remote,
        country,
        posted_at::DATE AS posted_date,

        -- Salary
        annual_min_salary,
        annual_max_salary,
        ROUND((COALESCE(annual_min_salary, 0) + COALESCE(annual_max_salary, 0)) / 2, 2) AS annual_avg_salary,
        salary_period,
        CASE
            WHEN annual_max_salary IS NULL THEN 'No salary info'
            WHEN annual_avg_salary < 80000  THEN 'Junior'
            WHEN annual_avg_salary < 120000 THEN 'Mid-level'
            WHEN annual_avg_salary < 160000 THEN 'Senior'
            ELSE 'Staff / Principal'
        END AS salary_band,

        -- Seniority from title
        CASE
            WHEN LOWER(job_title) LIKE '%senior%' OR LOWER(job_title) LIKE '%sr.%'  THEN 'Senior'
            WHEN LOWER(job_title) LIKE '%junior%' OR LOWER(job_title) LIKE '%jr.%'  THEN 'Junior'
            WHEN LOWER(job_title) LIKE '%lead%'   OR LOWER(job_title) LIKE '%staff%' THEN 'Lead / Staff'
            WHEN LOWER(job_title) LIKE '%principal%'                                  THEN 'Principal'
            WHEN LOWER(job_title) LIKE '%manager%'                                    THEN 'Manager'
            ELSE 'Mid-level'
        END AS seniority,

        -- Skill signals from description
        CASE WHEN LOWER(description) LIKE '%python%'     THEN TRUE ELSE FALSE END AS req_python,
        CASE WHEN LOWER(description) LIKE '%sql%'        THEN TRUE ELSE FALSE END AS req_sql,
        CASE WHEN LOWER(description) LIKE '%dbt%'        THEN TRUE ELSE FALSE END AS req_dbt,
        CASE WHEN LOWER(description) LIKE '%spark%'      THEN TRUE ELSE FALSE END AS req_spark,
        CASE WHEN LOWER(description) LIKE '%airflow%'    THEN TRUE ELSE FALSE END AS req_airflow,
        CASE WHEN LOWER(description) LIKE '%kafka%'      THEN TRUE ELSE FALSE END AS req_kafka,
        CASE WHEN LOWER(description) LIKE '%kubernetes%' OR LOWER(description) LIKE '%k8s%' THEN TRUE ELSE FALSE END AS req_kubernetes,
        CASE WHEN LOWER(description) LIKE '%aws%'        THEN TRUE ELSE FALSE END AS req_aws,
        CASE WHEN LOWER(description) LIKE '%gcp%' OR LOWER(description) LIKE '%bigquery%' THEN TRUE ELSE FALSE END AS req_gcp,
        CASE WHEN LOWER(description) LIKE '%azure%'      THEN TRUE ELSE FALSE END AS req_azure

    FROM salary_annualized
)

SELECT * FROM enriched
