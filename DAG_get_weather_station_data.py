#
# Author: Team Mojo
# Course: DWL03-Dat Lakes and Data Warehouses
# Data: Autumn 2021
#
# Description: An Airflow DAG to GET and POST data from weather station
#              to Axpo's Azure Data Lake

from datetime import timedelta, datetime
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import json
import requests
import pandas as pd
import numpy as np
import axe.auth as auth

engine_azure = create_engine(auth('di-dbs-conn'))
metadata = MetaData(bind=engine_azure)
schema = 'weather_station'
Session = sessionmaker(bind=engine_azure)
session = Session()

domain = auth('dcent-domain')
api_key = auth('dcent-api-key')

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
def get_weather_data():
    """
        Get weather data from Weather station in Fällanden
    """

    # ----------------------------- Supporting Functions -----------------------------
    def _ix2df(series):
        df = pd.DataFrame(series['values'], columns=series['columns'])
        df['series'] = series['tags']['uqk']
        return df

    def create_delta_list(df, session):

        table_name = Table('weather_data', metadata, schema=schema, autoload=True)
        exist_data = pd.read_sql(session.query(table_name).statement, session.bind)
        exist_data['ts'] = pd.to_datetime(exist_data['ts'], utc=True)
        temp = pd.merge(df, exist_data, on=['ts', 'SignalID', 'DeviceID'], how='left', indicator='Exist')
        temp['Exist'] = np.where(temp.Exist == 'both', True, False)
        temp = temp[(temp.Exist == False)]
        delta_df = temp.drop(['Exist', 'value_y'], 1)
        delta_df.rename(columns={'value_x': 'value'}, inplace=True)

        return delta_df

    # ----------------------------- Task Functions -----------------------------
    @task()
    def query(domain, api_key, time_filter='',
              device='//', location='//',
              sensor='//', include_network_sensors=False,
              channel='//',
              agg_func=None, agg_interval=None):

        select_var = 'value'
        fill = ''
        interval = ''

        if agg_func is not None:
            select_var = agg_func + '("value") as value'
            fill = 'fill(null)'

        if agg_interval is not None:
            interval = ', time(%s)' % agg_interval

        if time_filter != '':
            time_filter = ' AND ' + time_filter

        filter_ = (' location =~ %s'
                   ' AND node =~ %s'
                   ' AND sensor =~ %s'
                   ' AND ((channel =~ %s OR channel !~ /.+/)'
                   ' %s)') % (location,
                              device,
                              sensor,
                              channel,
                              ('' if include_network_sensors
                               else 'AND channel !~ /^link-/'))

        q = ('SELECT %s FROM "measurements" '
             ' WHERE %s %s'
             ' GROUP BY "uqk" %s %s') % (select_var,
                                         filter_,
                                         time_filter,
                                         interval,
                                         fill)

        r = requests.get('https://%s/api/datasources/proxy/1/query' % domain,
                         params={'db': 'main',
                                 'epoch': 'ms',
                                 'q': q},
                         headers={'Authorization': 'Bearer %s' % api_key})

        data = json.loads(r.text)

        if 'results' not in data or 'series' not in data['results'][0]:
            raise ValueError("No series returned: %s" % r.text)
        else:
            return data

    @task()
    def save_data_to_db(data: dict):

        df = pd.concat(_ix2df(s) for s in data['results'][0]['series'])
        df['time'] = pd.to_datetime(df['time'], unit='ms', utc=True)
        try:
            df['time'] = df['time'].dt.tz_localize('UTC')
        except TypeError:
            pass
        df['time'] = df['time'].dt.tz_convert('UTC')

        df = df.set_index(['series', 'time'])
        df = df.reset_index()

        df['device'] = df.series
        df.device = df.device.apply(lambda x: x.split(".")[0])
        df.series = df.series.apply(lambda x: x.split(".")[1])

        columns = df.series.unique().tolist()
        mapping = {}
        count = 1

        for col in columns:
            mapping[col] = count
            count += 1

        df["SignalID"] = df["series"].map(mapping)
        df = df.drop('series', 1)
        df = df.rename(columns={"device": "DeviceID", "time": "ts"})
        df.DeviceID = df.DeviceID.astype("int64")

        delta_df = create_delta_list(df, session)
        delta_df.to_sql('weather_data', schema=schema, con=engine_azure, if_exists='append', index=False)

    data = query(domain=domain, api_key=api_key)
    save_data_to_db(data)


get_weather_data = get_weather_data()