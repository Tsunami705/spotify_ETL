# spotify_ETL
Extract my spotify 50 listened songs daily and load into postgresql database.

Application used:Airflow,Docker

## ETL Process

### Extract
Bash script to send request to spotify api https://developer.spotify.com/console/get-recently-played/ and get json file,store in the local disk.
We pick only the data we need(song names,musicials,time we listen to) save it into csv in the dataframe format.

### Transform(Validate)
We check if the data we extract is avaliable.The primary key is unique,there is no null in the data,and all timestamps are of yesterday's date.

### Load
We save the data into the local postgresql using sqlalchemy,psycopg2,pandas

## Docker,Airflow setting
We can use airflow in a local isolated environment, or we can use airflow in a container.

Using the container version of airflow, in order to communicate with the local postgresql, you need to set the network mode to host and add a volume.

Here is the guide to install dockerized Airflow:

https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

https://airflow.apache.org/docs/apache-airflow/stable/start.html

The airflow setting file in the local isolation environment is in $AIRFLOW_HOME (the default is ~/airflow), where you can modify the storage folders of dags, plugins, logs, and other environment variables.

Tutorial for installing airflow locally：

https://www.youtube.com/watch?v=i25ttd32-eo&list=PLNkCniHtd0PNM4NZ5etgYMw4ojid0Aa6i&index=5
