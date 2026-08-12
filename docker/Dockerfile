FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app/dbt

WORKDIR /app

COPY docker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt/ ./dbt/
COPY ingestion/ ./ingestion/

WORKDIR /app/dbt

# image - dbt runner. ENTRYPOINT fixes the tool, CMD sets the default subcommand
# Override CMD for other dbt commands, for example
#   docker run --env-file .env grocery-dbt test
#   docker run --env-file .env grocery-dbt run --select dim_date
# To run the ingestion capability (overriding the entrypoint)
#   docker run --env-file .env --entrypoint python grocery-dbt ../ingestion/load_to_snowflake.py
ENTRYPOINT ["dbt"]
CMD ["build"]
