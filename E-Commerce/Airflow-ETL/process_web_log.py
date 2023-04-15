# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import re
import airflow
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'salah',
    'start_date':datetime.datetime(2023, 4, 14),
    'email': 'assuage.135@gmail.com'
}

dag = DAG(
    'process_web_log',
    default_args=default_args,
    description = 'pipeline that analyzes the web server log file,\
        extracts the required lines(ending with html) and fields(time stamp, size )\
            and transforms (bytes to mb) and load (append to an existing file.)',
    schedule = '@daily')



def extract_ip_adress(ti):
    """
    

    Parameters
    ----------
    text_file : TYPE
        this is web server log file.

    Returns
    -------
    text file contains ip addresses of ecah web server log.

    """
    
    input_file = '/home/salah/Documents/Data-Engineering/E-Commerce/accesslog.txt'
    extracted_data = '/home/salah/Documents/Data-Engineering/E-Commerce/extracted_data.txt'
    
    with open(input_file,'r') as text:
        contents = text.read()
        
        ip_pattaren = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')
        ip_adresses = ip_pattaren.findall(contents)
        
        
    with open(extracted_data,'w') as out_text:
        for ip_adress in ip_adresses:
            out_text.write(ip_adress + '\n')
            print(ip_adress)
    
    ti.xcom_push(key='extracted_data',value = extracted_data)
            
extract_data = PythonOperator(
    task_id = 'extract_data',
    python_callable = extract_ip_adress,
    dag = dag
    )

def transform_ip_adress(ti):
    """
    

    Parameters
    ----------
    text_file : TYPE
        this is web server ip adresses file.

    Returns
    -------
    filter out all the occurrences of ipaddress “198.46.149.143” from extracted_data.txt.

    """
    
    input_file = ti.xcom_pull(task_ids = "extract_data",key = 'extracted_data')
    
    transformed_data = '/home/salah/Documents/Data-Engineering/E-Commerce/transformed_data.txt'
    
    with open(input_file,'r') as text:
        lines = text.read().split('\n')
    
    with open(transformed_data,'w') as out_text:
        for line in lines:
            if line == '198.46.149.143':
                out_text.write(line + '\n')
                print(line)  
                    
    ti.xcom_push(key='transformed_data',value = transformed_data)
    
    
    
    
transform_data = PythonOperator(
    task_id = 'transform_data',
    python_callable = transform_ip_adress,
    dag = dag
    ) 
    

load_data = BashOperator(
    task_id = 'load_data',
    bash_command = 'tar -czf /home/salah/Documents/Data-Engineering/E-Commerce/transformed_data.tar.gz /home/salah/Documents/Data-Engineering/E-Commerce/transformed_data.txt',
    dag = dag
    ) 
   


extract_data >> transform_data >> load_data