from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.http_hook import HttpHook

from airflow.exceptions import AirflowException
from datetime import datetime
import os
import json
from hdfs import InsecureClient


class ComplexHttpOperator(SimpleHttpOperator):
    def __init__(self, save_on_disk=False, *args, **kwargs):
        super(ComplexHttpOperator, self).__init__( *args, **kwargs)
        self.save_on_disk = save_on_disk
        
    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        client = InsecureClient('http://127.0.0.1:50070', user='user')
        self.log.info("Calling HTTP method")
        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)

        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        
        
        if self.save_on_disk:
            
            path = os.path.join('/','datalake','bronze','api_data',str(self.data['date']))
            client.makedirs(path)  
            
            
            
            with client.write(os.path.join(path,'rd_api_data.json'),encoding='utf-8') as json_file:
                self.log.info('Writing data from API to hdfs')
                data = response.json()
                
                json.dump(data, json_file)
          
        if self.xcom_push_flag:
            return response.text
        