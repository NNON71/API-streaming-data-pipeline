from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'airscholar',
    'start_date': datetime(2024, 12, 13, 10, 00)
}

def get_data():
    import requests
    
    response = requests.get('https://randomuser.me/api/') 
    response = response.json()
    response = response['results'][0]
    
    return response

def format_data(res) :
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}."
    data['postcode'] = res['location']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_data'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data

def stream_data() :
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    # print(json.dumps(res, indent=3))
    current_time = time.time()
    
    while True:
        if time.time() > current_time + 60 : # 1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
    
            producer.send('user_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.errorf(f"An error occured : {e}")
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag : 
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
    
# stream_data()