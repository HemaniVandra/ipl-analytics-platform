# Great Expectations

GE runs inside Databricks notebook `07_ge_deliveries_suite`.

## Storage location on Databricks
All GE artifacts are stored in Unity Catalog Volume:
/Volumes/ipl_catalog/raw_data/great_expectations/
├── expectations/deliveries_suite.json   ← expectation suite
├── validations/                         ← run history
└── data_docs/local_site/index.html      ← HTML report

## Running
- Development : run notebook manually in Databricks
- Production  : triggered by Airflow nightly_pipeline DAG
                task: run_ge_checkpoint

## Viewing results
Open data docs in Databricks:
displayHTML(open('/Volumes/ipl_catalog/raw_data/
great_expectations/data_docs/local_site/index.html').read())