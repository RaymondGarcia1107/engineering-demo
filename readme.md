# Engineering Demo

I wrote this project to demo some simple data engineering. The project begins with a source database, hosted on an EC2 in AWS. The production database is a postgres database running in a docker container.
Synthetic data is seeded into the production database using python scripts. From there, I wrote python scripts to create a rudimentary pipeline, moving changed data into S3 as a datalake.
Finally I used python to move the data from S3 to RDS into staging tables, and then dbt to transform the data from staging to analytics
I am using Airflow to orchestrate everything.
Docker is used to load the project onto a linux server and docker-compose up

## App

The app folder contains two python scripts that act as the initial data load scripts. They create a table using SQL, then
generate synthetic data to populate the tables. I also generated a dailydataload.py file to periodically push fresh data to the database and test change management

## ETL

The etl folder is where the pipeline files are.

- batch_s3_etl.py

  - This file holds the base function to move any table from the production database into S3.
  - The data is loaded into pandas, then stored as a parquet file and uploaded to an S3 folder.
  - It is intended to be triggered manually, as it pulls the entire table.
  - The table is saved in s3 in a child folder within the main folder (Table)

- batch_rds_etl.py

  - This file is responsible for loading the parquet files from S3 and moving them to RDS.
  - It loops through each folder within the main table folder, and loads any parquet files inside the folders. It then appends the data into one dataframe with pyarrow.
  - Finally the data is loaded into the staging table in RDS
  - This script is also intended to trigger manually, in tandem with a batch_s3_etl.py run.

- incremental_s3_etl.py

  - This function handles data changes within the prod database.
  - The production database tracks changes with **updated_at**. We use this to check for any rows that have a change since the last sync.
  - Changes are gathered, the last sync variable is updated, and the new records are written to S3 in a new folder within the table folder

- incremental_rds_hist_etl.py
  - This function manages the new changes and merges them into staging.
  - It first pulls all possible parquet files from s3, then compares them to what has already been loaded into RDS.
  - New files are then loaded into a pyarrow dataframe and sent to the staging table

## Airflow

The airflow folder manages my dags. I have 4 dags currently listed.

- batch_transactions_full_update_dag.py/batch_users_full_update_dag.py

  - This file manages the orchestration for a full batch update for the given table.
  - It first runs batch_s3_etl
  - Then it runs a SQL script to truncate the table
  - Finally it runs incremental_rds_hist_etl
  - This function is intended to be ran manually

- incremental_transactions_dag.py/incremental_users_dag.py
  - This file manages the orchestration for an incremental update for the given table.
  - It first runs incremental_s3_etl
  - Then it runs incremental_rds_hist_etl
  - This dag is expected to run on a schedule determined by the business users

## DBT

The dbt folder houses the transformation logic for transforming the data from staging to analytics. From the analytics schema, we can begin to build dashboards and analyze the data

#### Models

The models folder houses the two dbt models that run. One for transcation and the other for users. The transformations are simple for now. We do the following:

- If the model run is an incremental run, allow dbt to manage deduping and load the data into the database.
- If the model is not an incremental run, partition the table and take the latest update for each record based on **updated_at**
- Make useful fields lowercase.
  Currently the models are not loaded into any DAGs and are intended to be ran manually after each update.
