# LendFlow

Credit & Collections analytics pipeline for fintech. End-to-end data engineering project built on AWS lakehouse architecture.

## Stack

- **Processing:** PySpark 3.5 + Delta Lake
- **Storage:** AWS S3 (bronze / silver / gold)
- **Catalog:** AWS Glue Data Catalog
- **Warehouse:** AWS Athena
- **Transformations:** dbt-athena
- **Orchestration:** Apache Airflow
- **Dashboard:** Evidence.dev (HTML storytelling)
- **IaC:** Terraform
- **CI/CD:** GitHub Actions
- **Data Quality:** dbt tests + Elementary

## Dataset

Lending Club loan data (2007-2018), ~2M loans with originations, payments, delinquency status, borrower profile.

## Domain

Fintech credit lifecycle: originations, portfolio performance, delinquency buckets, roll rates, vintage curves, recovery analytics.

## Status

🚧 Work in progress

## Author

Carlos Leguizamon Guillaumet

LinkedIn
