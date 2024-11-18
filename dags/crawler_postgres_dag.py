from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from scripts.crawler import crawl_website, extract_headings, count_keyword_occurrences, monitor_keywords_on_websites
from airflow.providers.postgres.operators.postgres import PostgresOperator
import json

default_args = {
    'owner':'Hans Atonen',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def call_test_script(ti,*op_args):
    keywords = ['election', 'war', 'economy']
    websites = ['https://www.ft.com/', 'https://www.theguardian.com/europe']
    results = monitor_keywords_on_websites(keywords, websites)
    ti.xcom_push(key='crawler_results', value=results)

def get_confirmation(*op_args):
    timestamp_datetime = datetime.fromisoformat(op_args[0])
    current_day = timestamp_datetime.weekday()
    current_hour = timestamp_datetime.hour
    # Check if it's Friday or if it's any other day but the hour is divisible by 2
    if current_day == 4 or (current_day != 4 and current_hour % 2 == 0):
        return True
    else:
        return False

with DAG(
    dag_id='crawler_postgres_dag',
    default_args=default_args,
    description='Crawler that works Saturday to Thursday and runs every two hours and runs every hour on Fridays',
    start_date=datetime(2024, 5, 10),
    schedule_interval='0 * * * *',#'0 */2 * * 0-4,6',
    render_template_as_native_obj=True
) as dag:
    crawler = PythonOperator(
        task_id='run_test',
        python_callable=call_test_script
    )

    start_op = ShortCircuitOperator(
        task_id="get_confirmation",
        python_callable=get_confirmation,
        op_args=['{{ts}}']
    )

    table_creation = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_local',
        sql="""
            CREATE TABLE if not exists hometask_table (
                datetime TIMESTAMP,
                json jsonb
            )
        """
    )

    data_deletion = PostgresOperator(
        task_id='delete_from_table',
        postgres_conn_id='postgres_local',
        sql="""
            DELETE FROM hometask_table where datetime = '{{ts}}' and json = '{{ti.xcom_pull(task_ids='run_test', key='crawler_results')}}'
        """
    )

    data_insertion = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_local',
        sql="""
            INSERT INTO hometask_table (datetime, json) VALUES('{{ts}}','{{ti.xcom_pull(task_ids='run_test', key='crawler_results')}}')
        """
    )


start_op >> crawler >> table_creation >> data_deletion >> data_insertion