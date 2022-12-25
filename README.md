# spotify_ETL
Extract my spotify 50 listened songs daily and load into postgresql database.

Application used:Airflow,Docker

## ETL Process

### Extract
Bash script to send request to spotify api https://developer.spotify.com/console/get-recently-played/ and get json file,store in the local disk.


