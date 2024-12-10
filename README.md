# Wind data

The Python pipeline cleans raw turbine data, calculates statistics, detects anomalies, and exports results. It handles daily updated CSV files for multiple turbines.

Along with an Airflow DAG to schedule and orchestrate the entire pipeline.

# Setup & Execution

## SETUP

* Install dependencies with: `pip install -r requirements.txt`
* Input: Place CSV files in a specified source directory (we haven't built a CLI so change the path for the source in `main.py#l15` )

## Execute the project

* Execution: Run `python main.py` from the project root.
* Output: CSV files saved in a specified destination directory – specified @ `main.py#l15`.

# How to Run Tests:
* `pip install -r requirements.txt` if you haven't
* From the project root directory, run: `pytest tests`

# Recommendations for a production ready project

Deploy the orchestration on a Airflow server (use Astronomer, AWS MWAA, Google Cloud Composer or Workflow Orchestration Manager in Azure Data Factory with the airflow flavor), deploy the DAG and the code in the airflow modules, store the sources + sink in a discrete S3 bucket / GCS / blob, build a CI/CD to update the airflow DAG + code if needed, use App Config / Secrets Manager or Key Vault to bind the configuration or secret values that may be required and add nice downstream OLAP layer (Athena, BigQuery or Synapse) on top of your S3 compatible sink to visualise it if needed :) I can help to build all of this :)


