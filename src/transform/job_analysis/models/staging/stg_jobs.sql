{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT * FROM read_json_auto('C:/Users/Yago/Documents/projects/job-data-pipeline/data/raw/*.json')
),

unnested_data AS (
    SELECT unnest(data) as job FROM raw_data
)

SELECT
    job.job_id::VARCHAR as job_id,
    job.job_title::VARCHAR as job_title,
    job.employer_name::VARCHAR as company_name,
    job.job_is_remote::BOOLEAN as is_remote,
    job.job_country::VARCHAR as country,
    to_timestamp(job.job_posted_at_timestamp) as posted_at,
    job.job_min_salary::FLOAT as min_salary,
    job.job_max_salary::FLOAT as max_salary,
    job.job_salary_period::VARCHAR as salary_period,
    job.job_description::VARCHAR as description
FROM unnested_data