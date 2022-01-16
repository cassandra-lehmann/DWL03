#
# Author: Team Mojo
# Course: DWL03-Dat Lakes and Data Warehouses
# Data: Autumn 2021
#
# Description: Post Weather station to AWS S3 bucket

from datetime import timedelta, datetime
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import axe.auth as auth
from dotenv import load_dotenv
import os
import boto3
import pandas as pd
import json

load_dotenv()

aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')

engine_azure = create_engine(auth('di-dbs-conn'))
metadata = MetaData(bind=engine_azure)
schema = 'weather_station'
Session = sessionmaker(bind=engine_azure)
session = Session()

default_args = {
    'owner': 'casslehm',
    'start_date': datetime(2021, 12, 20),
    'depends_on_past': False,
    'email_on_failure': True,
    'email': 'cassandra.lehmann@axpo.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2))
def weather_station_data_post():
    @task()
    def save_data_to_db():
        table_name = Table('weather_data', metadata, schema=schema, autoload=True)
        df = pd.read_sql(session.query(table_name).statement, session.bind)
        data = dict(df.values)

        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key)

        uploadByteStream = bytes(json.dumps(data).encode('UTF-8'))
        s3_client.put_object(Bucket='dwl03-bucket', Key='weather_station.json', Body=uploadByteStream)

        os.remove("temp.json")

weather_station_data_post()