from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {

    'owner':'abdo',
    'start_date':days_ago(1)

}

def get_data():
    import json,requests
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    
    return res

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data




def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    res = get_data()
    res= format_data(res)

    # print(json.dumps(res, indent=3))
    
    producer = KafkaProducer(bootstrap_servers =['broker:29092'],max_block_ms=5000)
    cur_time = time.time()
    while True:
        if time.time() > cur_time + 60:
            break
        try:
            res = get_data()
            res= format_data(res)
            producer.send('users',json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f"error occuerd {e}")
            continue

with DAG('streaming',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(

        task_id='stram_data_from_api',
        python_callable=stream_data
        
    )

streaming_task