import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import json
import csv
import pandas as pd
import sqlalchemy as sc
import psycopg2

todays_date=str(datetime.datetime.today())[0:10]
download_limit=50
airflow_home=os.environ.get("AIRFLOW_HOME")
data_save_path="/home/tsunami/Desktop/spotify_ETL/de-project/airflow/spo_data_warehouse"
token="BQD4oW39MoqnJmC3pT_sJBIVp-vkqG3ZC7dsMH9hYs6ogpJByCJJpMPGem7bwI7_J6GF0BmpeTKFaTVfaT-_2OGn9fj1FzmOJDZloIrl_w--jCpC7ueSVVFEZYnM21Qtg2CA7L2TUPRKDzsvl2U_Mhh76e53QJAXoFES0QH9CLpVv1kf4fQSqCVL6sQB6P1BPIgTYxZTlKoC"

today=datetime.datetime.now()
yesterday=today-datetime.timedelta(days=1)
yesterday_unix_timestamp=int(yesterday.timestamp())*1000

def run_extract(data_save_path,todays_date):
    with open(f"{data_save_path}/spo_data_{todays_date}.json",'r') as j:
        data=json.load(j)

        song_names = []
        artist_names = []
        played_at_list = []
        timestamps = []

        for song in data["items"]:
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            played_at_list.append(song["played_at"])
            timestamps.append(song["played_at"][0:10])

        song_dict = {
            "song_name" : song_names,
            "artist_name": artist_names,
            "played_at_list" : played_at_list,
            "timestamp" : timestamps
        }

        song_df=pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at_list", "timestamp"])

        song_df.to_csv(f"{data_save_path}/spo_data_{todays_date}.csv",index=False)

        print('extract data success.')

def run_transform():
    df=pd.read_csv(f"{data_save_path}/spo_data_{todays_date}.csv")
    if pd.Series(df['played_at_list']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') < yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    print('Transform data success.')

def run_load(database,user,password,host,port):
    song_df=pd.read_csv(f"{data_save_path}/spo_data_{todays_date}.csv")
    engine = sc.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}',echo=True)
    #if the database doesn't exist,it will create it automatically
    #create connection
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    print("Connect successful.")

    cursor=conn.cursor()    #operate database
    sql_query='''
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200) NOT NULL,
        artist_name VARCHAR(200),
        played_at_list VARCHAR(200) UNIQUE,
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at_list)
    )
    
    '''
    cursor.execute(sql_query)
    conn.commit()   #commit to postgresql

    song_df.to_sql("my_played_tracks",con=engine,index=False,if_exists='append')

    #try:
    #    song_df.to_sql("my_played_tracks",con=engine,index=False,if_exists='append')
    #except:
    #    print("Data already exists in the database")

    conn.close()
    print("Close database successfully.")


default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':days_ago(1),
    'email':['airflow@example.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':datetime.timedelta(minutes=1)
}

with DAG(
    dag_id='spotify_dag',
    default_args=default_args,
    description='Our first dag',
    schedule_interval=datetime.timedelta(days=1)
)as dag:


    download_dataset_task = BashOperator(
        task_id='download_dataset_task',
        bash_command=f"curl -X \"GET\" \"https://api.spotify.com/v1/me/player/recently-played?limit={download_limit}&after={yesterday_unix_timestamp}\" -H \"Accept: application/json\" -H \"Content-Type: application/json\" -H \"Authorization: Bearer {token}\" > {data_save_path}/spo_data_{todays_date}.json"
    )

    extract_data=PythonOperator(
        task_id='extract_data',
        python_callable=run_extract,
        op_kwargs={
            'data_save_path':data_save_path,
            'todays_date':todays_date
        }
    )

    transform_data=PythonOperator(
        task_id="transform_data",
        python_callable=run_transform
    )

    load_data=PythonOperator(
        task_id="load_data",
        python_callable=run_load,
        op_kwargs={
            'database':'spotify_project',
            'user':'postgres',
            'password':'1111',
            'host':'localhost',
            'port':'5432'
        }
    )

    download_dataset_task >> extract_data >> transform_data >> load_data





