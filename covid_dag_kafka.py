from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
import json

default_args = {
                'owner': 'airflow',
                'start_date': days_ago(1),
                }

KAFKA_TOPIC = 'covid_3'

dag = DAG(
            'covid_pipeline_kafka',
            default_args=default_args,
            description='An Airflow DAG to Visualize Kafka Covid data',
            schedule_interval=None,
            catchup=False
            )

def process_data(**kwargs):
    print("Kwargs >>>>>", kwargs)
    daily_confirmed = kwargs['dag_run'].conf.get('dailyconfirmed')
    daily_deceased = kwargs['dag_run'].conf.get('dailydeceased')
    daily_recovered = kwargs['dag_run'].conf.get('dailyrecovered')
    total_confirmed = kwargs['dag_run'].conf.get('totalconfirmed')
    total_deceased = kwargs['dag_run'].conf.get('totaldeceased')
    total_recovered = kwargs['dag_run'].conf.get('totalrecovered')

    current_date = datetime.now().strftime("%d %B %Y %H:%M:%S")
    current_date_ymd = datetime.now().strftime("%Y-%m-%d")

    data = {
        "dailyconfirmed": daily_confirmed,
        "dailydeceased": daily_deceased,
        "dailyrecovered": daily_recovered,
        "date": current_date,
        "dateymd": current_date_ymd,
        "totalconfirmed": total_confirmed,
        "totaldeceased": total_deceased,
        "totalrecovered": total_recovered
    }

    ti = kwargs['ti']
    ti.xcom_push(key='data',value=data)

    return data

def producer_function(data : dict):
    # yield json.dumps(data), json.dumps({'data': data})
    yield json.dumps('data_1'), json.dumps({'data': data})

# def export_data_to_file(ti):
#     with open('myfile.txt', 'w') as fp:
#         pass

def consumer_function(message):
    # print("Start here ************ ")
    # print("Json ********** ", message.key(), message.value(), message.topic())
    key = json.loads(message.key())
    # print("Message here ********** ")
    message_content = json.loads(message.value())
    print(f"Key >> {key} , Message >> {message_content}" )

Create_Message = PythonOperator(
                                task_id='Create_Message', 
                                python_callable=process_data,
                                dag=dag,
                                provide_context=True
                                )

Kafka_Producer = ProduceToTopicOperator(
                                        task_id='Enter_data_to_kafka_topic',
                                        topic=KAFKA_TOPIC,
                                        producer_function=producer_function, 
                                        producer_function_args=["{{ ti.xcom_pull(key='data') }}"],
                                        poll_timeout=10,
                                        dag=dag,
                                        kafka_config={"bootstrap.servers": "localhost:9092"}
                                        )

Kafka_Consumer = ConsumeFromTopicOperator(
                                        task_id="Get_data_from_kafka_topic",
                                        topics=[KAFKA_TOPIC],
                                        apply_function=consumer_function,
                                        max_messages=10,
                                        poll_timeout=10,
                                        dag=dag,
                                        consumer_config={'bootstrap.servers': 'localhost:9092',
                                                            'group.id': 'streamer1',
                                                            'auto.offset.reset': 'smallest'},
                                        commit_cadence="never"
                                    )

Create_Message >> Kafka_Producer >> Kafka_Consumer
