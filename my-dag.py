#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Alex P',
}

with DAG(
    dag_id='airflow_hw',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['hw'],
) as dag:
   
    submit_job = SparkSubmitOperator(
        application="${SPARK_HOME}/examples/src/main/python/pi.py", task_id="submit_job"
    )
