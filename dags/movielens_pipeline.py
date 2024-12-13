from airflow import DAG # type: ignore 
from airflow.operators.python import PythonOperator # type: ignore
from airflow.utils.dates import days_ago # type: ignore
from download_data import download_and_extract_data
from split_data import split_data_80
from upload_to_minio import upload_to_minio
from train_and_predict import train_and_predict

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'movielens_pipeline',
    default_args=default_args,
    description='Pipeline for MovieLens dataset',
    schedule_interval=None,
)

download_task = PythonOperator(
    task_id='download_unpack_data',
    python_callable=download_and_extract_data,
    dag=dag,
)

split_task = PythonOperator(
    task_id='split',
    python_callable=split_data_80,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_to_minio,
    dag=dag,
)

train_task = PythonOperator(
    task_id='train_and_predict',
    python_callable=train_and_predict,
    dag=dag,
)

download_task >> split_task >> upload_task >> train_task