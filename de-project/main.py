import sqlalchemy as sc
from sqlalchemy import create_engine
import pandas as pd
#from sqlalchemy import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import psycopg2

DATABASE_LOCATION='sqlite:///my_played_tracks.sqlite'
USER_ID="Sumnery"
TOKEN="BQDqh8nR-5meiEMHMXBwgcMyCcgPSqAcFcKfdXinyKse6PTbSAimKkjP_S89F77pXKd-DYpDzx9WnWxneLSiXo_I9WyNyCUwYUXQ5nGcguksL-Hi91nW8_MKToS5wFvZ4rPqGfN719ofZc9mnSCd_wGYIsWOKT62Vco3mI0MjClH50dF_xj_r11nAO721dUnOkdeqt6ago0F"

#Transform
def check_if_valid_data(df:pd.DataFrame) -> bool:
    #check if dataframe is empty
    if df.empty:
        print("No songs downloaded.Finishing execution.")
        return False
    
    #Primary Key Check(确保主键是独一无二的)
    if pd.Series(df["played_at_list"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated.")
    
    #Check for nulls
    if df.isnull().values.any():
        raise Exception("Null value found.")
    
    #Check that all timestamps are of yesterday's date
    yesterday=datetime.datetime.now()-datetime.timedelta(days=1)
    yesterday=yesterday.replace(hour=0,minute=0,second=0,microsecond=0)
    #print(yesterday)

    timestamps=df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp,"%Y-%m-%d") < yesterday:
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")

    return True


if __name__=="__main__":

    #Extract
    headers={
        "Accept":"application/json",
        "Content-Type":"application/json",
        "Authorization":"Bearer {token}".format(token=TOKEN)
    }

    today=datetime.datetime.now()
    yesterday=today-datetime.timedelta(days=1)
    yesterday_unix_timestamp=int(yesterday.timestamp())*1000

    r=requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}&limit=50".format(time=yesterday_unix_timestamp),headers=headers)

    data=r.json()
    #print(data)

    song_names=[]
    artist_names=[]
    played_at_list=[]
    timestamps=[]

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    #将数据处理成pandas dataframe形式
    #先处理成字典
    song_dict={
        "song_name":song_names,
        "artist_name":artist_names,
        "played_at_list":played_at_list,
        "timestamp":timestamps
    }

    song_df=pd.DataFrame(song_dict,columns=["song_name",
        "artist_name","played_at_list","timestamp"])

    #Validate
    if check_if_valid_data(song_df):
        print("Data valid,proceed to Load stage")
        print(song_df)


    #Load
    engine = sc.create_engine(f'postgresql://postgres:1111@localhost:5432/spotify_project',echo=True)
    #if the database doesn't exist,it will create it automatically
    #创建连接
    conn = psycopg2.connect(database="spotify_project", user="postgres", password="1111", host="localhost", port="5432")
    print("Connect successful.")
    cursor=conn.cursor()    #通过cursor方法对数据库操作

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
    conn.commit()   #向postgreSQL提交命令

    try:
        song_df.to_sql("my_played_tracks",con=engine,index=False,if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully.")