#!/bin/bash
set -e

echo ">>> Creating pipeline_db database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE pipeline_db;
EOSQL

echo ">>> Applying pipeline schema to pipeline_db..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "pipeline_db" -f /schema.sql

echo ">>> Database initialisation complete. Databases: airflow_db (Airflow metadata), pipeline_db (pipeline data)"
