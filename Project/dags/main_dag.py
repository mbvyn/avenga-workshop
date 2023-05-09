from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 9),
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
}

# Define the DAG
dag = DAG('main_dag', default_args=default_args, schedule_interval=None)

# Define the paths to the PySpark scripts
spark_producer = "/opt/airflow/app/spark_producer.py"
spark_consumer = "/opt/airflow/app/spark_consumer.py"
cassandra_to_elk = "/opt/airflow/app/cassandra_to_elk.py"

# Define the SparkOperator tasks
write_to_kafka = SparkSubmitOperator(
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2',
    task_id='write_to_kafka',
    application=spark_producer,
    conn_id='spark_default',
    verbose=False,
    dag=dag
)

write_to_cassandra = SparkSubmitOperator(
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,' \
             'com.datastax.spark:spark-cassandra-connector_2.12:3.2.0',
    task_id='write_to_cassandra',
    application=spark_consumer,
    conn_id='spark_default',
    verbose=False,
    dag=dag
)

write_to_kibana = SparkSubmitOperator(
    packages='com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,' \
             'org.elasticsearch:elasticsearch-spark-30_2.12:8.0.0',
    task_id='write_to_kibana',
    application=cassandra_to_elk,
    conn_id='spark_default',
    verbose=False,
    dag=dag
)

# Set the order of the tasks
write_to_kafka >> write_to_cassandra >> write_to_kibana
