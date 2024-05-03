# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.utils.dates import days_ago
# from datetime import datetime
from uuid import uuid4
from airflow.api.client.local_client import Client

DAG_ID = 'trigger-mage-pipeline'

run_id = str(uuid4())

try :
    c = Client(None, None)
    c.trigger_dag(dag_id=DAG_ID, run_id=run_id )
except Exception as err:
    print("Error : ", err)



#   conf={
#                                         "dailyconfirmed": 3,
#                                         "dailydeceased": 2,
#                                         "dailyrecovered": 1,
#                                         "totalconfirmed": 5,
#                                         "totaldeceased": 6,
#                                         "totalrecovered": 8
#                                     }


# default_args = {
#     'start_date': days_ago(1),
# }
# trigger_pipeline = TriggerDagRunOperator(
#                                         task_id='trigger_for_pipeline',
#                                         trigger_dag_id='covid_pipeline_kafka',
#                                         wait_for_completion=True,
#                                         trigger_run_id=run_id,
#                                         reset_dag_run=True
#     # conf={
#     #     "dailyconfirmed": 3,
#     #     "dailydeceased": 2,
#     #     "dailyrecovered": 1,
#     #     "totalconfirmed": 5,
#     #     "totaldeceased": 6,
#     #     "totalrecovered": 8
#     # }
# )