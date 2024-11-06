from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.operators.empty import Empty
from airflow.utils.dates import days_ago

REGION_NAME = Variable.get('REGION_NAME')
IAM_ROLE = Variable.get('IAM_ROLE_NAME')
SNOWFLAKE_USER = Variable.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = Variable.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = Variable.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = Variable.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = Variable.get('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = Variable.get('SNOWFLAKE_SCHEMA')
FIRE_INCIDENTS_URL = "https://data.sfgov.org/resource/wr8u-xric.csv"
RAW_TABLE = "staging_fire_incidents"


with DAG(
    'TN_challenge',
    description='DAG to invoke AWS Glue job for data extraction',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    start = Empty(
                task_id=f"start",
                )

    end = Empty(
                task_id=f"end",
                trigger_rule='all_done',
                )

    invoke_glue = AwsGlueJobOperator(
        task_id='invoke_glue_extraction',
        job_name='your_glue_job_name',
        script_location='s3://your-bucket/path/to/data_extraction_script.py',
        region_name=REGION_NAME,
        iam_role_name=IAM_ROLE,
        script_args={"--SNOWFLAKE_USER": "{{var.value.SNOWFLAKE_USER}}",
                     "--SNOWFLAKE_PASSWORD": "{{var.value.SNOWFLAKE_PASSWORD}}",
                     "--SNOWFLAKE_ACCOUNT": "{{var.value.SNOWFLAKE_ACCOUNT}}",
                     "--SNOWFLAKE_WAREHOUSE": "{{var.value.SNOWFLAKE_WAREHOUSE}}",
                     "--SNOWFLAKE_DATABASE": "{{var.value.SNOWFLAKE_DATABASE}}",
                     "--SNOWFLAKE_SCHEMA": "{{var.value.SNOWFLAKE_SCHEMA}}",
                     "--FIRE_INCIDENTS_URL": "{{var.value.FIRE_INCIDENTS_URL}}",
                     "--RAW_TABLE": "{{var.value.RAW_TABLE}}",
        }
    )

    start >> invoke_glue >> end