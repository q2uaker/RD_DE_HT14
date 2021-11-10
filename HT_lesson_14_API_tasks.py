from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator

import sys
sys.path.append('./HT14')
import json
import os
from HT14.config import Config
from HT14.complex_http_operator import ComplexHttpOperator
from HT14.api_to_silver import api_to_silver

config = Config(os.path.join('/','home','user','airflow','dags','HT14', 'config.yaml'))
config = config.get_config('HT14_app')

dag = DAG(
    dag_id="HT14_dag_APIrd",
    description="Hometask of 14th lesson API load",
    start_date = datetime(2021,6,19,12,00),
    schedule_interval = '@daily',
    user_defined_macros={
        'json': json
        }
    )

           
login_task = SimpleHttpOperator(
                       task_id = "robodreams_api_login_ht14",
                       dag = dag,
                       http_conn_id="rd_api",
                       headers = {"content-type": "application/json"},
                       method="POST",
                       endpoint= "auth",
                       data = json.dumps({"username": config['username'], "password": config['password']}),
                       xcom_push=True
                       )

load_to_bronze_task = ComplexHttpOperator(
                       task_id = "robodreams_api",
                       dag = dag,
                       http_conn_id="rd_api",
                       method="GET",
                       headers = {'content-type': 'text/plain',
                                  'Authorization': 'JWT ' + '{{json.loads(ti.xcom_pull(task_ids="robodreams_api_login_ht14", key="return_value"))["access_token"]}}',
                                  },
                       endpoint=config['data_point'],
                       data = {"date": str("{{ds}}")},
                       save_on_disk=True
                       )
load_to_silver_task =   PythonOperator(
                 task_id = "move_to_silver",
                 dag = dag,
                 python_callable = api_to_silver,
                 op_kwargs={},
                 provide_context = True
                )


login_task >> load_to_bronze_task >> load_to_silver_task