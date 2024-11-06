# TN-Challenge

This project processes and transforms fire incident data for analytical purposes, designed as a star schema in Snowflake using dbt.

## Project Overview
The first step in the ELT process is extracting the dataset from the source with Python. In this example, the approach was to use AWS services, as the target company uses AWS services daily. The code was developed to be used on AWS services as Glue, Lambda or Batch. In this excersice, Glue was selected as we did not have the expected execution time of the code (AWS Lambda has 15 minutes as execution time limitation).

To run the script daily, an Airflow DAG was created. The dag would be saved on s3, and then could be run from the AWS MWAA service (Airflow managed by AWS).

Regarding the transformation process, this dbt project structures raw fire incident data into a data warehouse schema with one fact table and multiple dimension tables. The star schema enables efficient analysis by linking incident metrics with descriptive details across various dimensions.

## Directory Structure of DBT Repo
- **models/load-fire-incidents-model/staging**: Contains models that transform raw data into a clean, standardized format.
- **models/load-fire-incidents-model/target_tables**: Contains final fact and dimension tables in the star schema.

## Data Model
- **Fact Table**: `fact_fire_incidents`
  - Stores incident-specific metrics, linked to dimension tables by foreign keys.
- **Dimension Tables**: `dim_date`, `dim_location`, `dim_incident_type`, `dim_unit`, `dim_damage_outcome`

## Important notes:
- The aim of the project was to demonstrate the approach used to find a solution for the challenge in one day of work, rather than developing a short solution and having more time testing it. The reason of this is to avoid having to consume time configuring free/trial versions of DBT, Airflow, and Snowflake.
- A docker image was generated but not used to test the solution.
- The project has two main folders, as the 'dbt_snowflake_solution' was aimed to be a separate repo to work as the DBT one.
- ChatGPT was used to help on documentation of the processes, as well as to create the report queries that would serve as an example of how the model would be consumed.
- The DBT transformation can be orchaestrated from DBT cloud, or with Airflow. In this excersice, it was not developed in Airflow.
- DAG variables are supposed to be added at the Airflow UI Variables tab, so that any changes in a variable value does not affect every single code developed.

## Possible ideas for improvement:
- Develop and add DBT tests to enforce data quality and validations
- Use GUIDs as the primary keys on Data Warehouse, to ensure uniqueness specially on the Fact Table.
- Add better documentation at each level of the process, including more complete docstrings on main python functions.
- Add a script to manipulate the staging table after the ELT process. This could be archive old information for a period of time or truncate the table, so the next insertion of data do not replicate all the previous rows.
