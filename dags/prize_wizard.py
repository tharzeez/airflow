from airflow import DAG, XComArg
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum
import dateutil.relativedelta
from datetime import datetime, date, timedelta
import calendar
import logging
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

getRegionQuery = """
    select bcom_scrape_search_queue_sqs_url as url,
    region_name as districtName,
    collection_identifier as collectionIdentifier,
    time_zone as timeZone, table_name_lowest_rates as tableName,
    concat(UPPER(region_name), ' Collection') as regionName,
    is_hard_delete_rate_enabled as isHardDeleteRateEnabled,
    hard_delete_max_rate as hardDeleteMaxRate,
    hard_delete_min_rate as hardDeleteMinRate,
    id as id, search_key, country,
    table_name_lowest_rates, table_name_prefix_algorithm,
    currency_code as currencyCode
    from booking_com_scrape_regions where country = 'vietnam' and collection_identifier IS NOT NULL and collection_identifier != '' and time_zone IS NOT NULL and time_zone != '' and currency_code IS NOT NULL and currency_code != ''

"""

def generate_next_40_days():
    # Get the current date
    current_date = datetime.now().date()

    next_40_days = []
    # Generate the next 40 days
    for i in range(0, 40):
        next_40_days.append({
            'created_ict_date': datetime.now().date().strftime('%Y-%m-%d'),
            'checkInDate': (current_date + timedelta(days=i)).strftime('%Y-%m-%d'),
            'days_to': i
        })
    return next_40_days

def construct_and_write_message():
    
    mysql_hook = MySqlHook(mysql_conn_id='pw-prod-db')
    region = mysql_hook.get_records(getRegionQuery)
    print('region', region)
    # second step for generating 40 days data for every region
    next_40_days = generate_next_40_days()

# Define the DAG
with DAG(
    dag_id='prize_wizard_daily_scheduler',
    # dag_id='weekly_data_insert',
    description='Daily scheduler',
    schedule_interval = None,
    start_date=pendulum.datetime(2023, 7, 14, tz="UTC"),
    catchup=False
) as dag:

    logging.debug('This is a debug message')
    # Task 1: Get all the Users from the database
    success_main = BashOperator(
        task_id='success_main',
        bash_command='echo "success_main"',
    )

    success_end = BashOperator(
        task_id='success_end',
        bash_command='echo "success_end"',
    )

    # writeMessage = SQLExecuteQueryOperator(
    #     task_id='update_calendar_table',
    #     conn_id='pw-prod-db',
    #     database='full_au_market_db',
    #     sql='''
    #         update calendar set is_airflow_ran = 1 where date_value >= "{{ task_instance.xcom_pull(task_ids="get_next_seven_days")[0][0] }}" and date_value <= "{{ task_instance.xcom_pull(task_ids="get_next_seven_days")[-1][0] }}" ''',
    #     dag=dag
    # )

    # invoke_lambda_function = LambdaInvokeFunctionOperator(
    #     task_id="invoke_lambda_function",
    #     aws_conn_id='calib_lambda',
    #     function_name="test_airflow_pw"
    #     # payload=json.dumps({"SampleEvent": {"SampleData": {"Name": "XYZ", "DoB": "1993-01-01"}}}),
    # )

    construct_and_write_message_task = PythonOperator(
        task_id='construct_and_write_message_task',
        python_callable=construct_and_write_message,
        provide_context=True,
        dag=dag
    )
    
    success_main >> construct_and_write_message_task >> success_end