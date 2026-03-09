FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# dbt profiles are injected via ConfigMap/Secret at runtime
ENV DBT_PROFILES_DIR=/app/profiles

CMD ["bash", "-c", "python src/extraction/extract_jobs.py && dbt run --project-dir src/transform/job_analysis --profiles-dir /app/profiles"]
