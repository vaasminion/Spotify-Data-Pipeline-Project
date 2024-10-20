import logging
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import requests
from spotipy import Spotify
from datetime import datetime
import json
import os
from boto3 import resource
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from airflow.models import Variable

#VARIABLE
S3_ENDPOINT = Variable.get('SPOTIFY_S3_ENDPOINT')
S3_ACCESSKEY = Variable.get('SPOTIFY_S3_ACCESSKEY')
S3_SECRETKEY =Variable.get('SPOTIFY_S3_SECRETKEY')
SPOTIFY_API_URL = Variable.get('SPOTIFY_API_URL')
POSTGRES_CONNECTION_STRING = Variable.get('POSTGRES_CONNECTION_STRING')
SPOTIFY_DB = Variable.get('SPOTIFY_DB')
SPOTIFY_DB_USER = Variable.get('SPOTIFY_DB_USER')
SPOTIFY_DB_PASSWORD = Variable.get('SPOTIFY_DB_PASSWORD')
BUCKET_NAME = Variable.get('BUCKET_NAME')

SCHEMA = StructType([StructField('context', StructType([StructField('external_urls', StructType([StructField('spotify', StringType(), True)]), True), StructField('href', StringType(), True), StructField('type', StringType(), True), StructField('uri', StringType(), True)]), True), StructField('played_at', StringType(), True), StructField('track', StructType([StructField('album', StructType([StructField('album_type', StringType(), True), StructField('artists', ArrayType(StructType([StructField('external_urls', StructType([StructField('spotify', StringType(), True)]), True), StructField('href', StringType(), True), StructField('id', StringType(), True), StructField('name', StringType(), True), StructField('type', StringType(), True), StructField('uri', StringType(), True)]), True), True), StructField('available_markets', ArrayType(StringType(), True), True), StructField('external_urls', StructType([StructField('spotify', StringType(), True)]), True), StructField('href', StringType(), True), StructField('id', StringType(), True), StructField('images', ArrayType(StructType([StructField('height', LongType(), True), StructField('url', StringType(), True), StructField('width', LongType(), True)]), True), True), StructField('name', StringType(), True), StructField('release_date', StringType(), True), StructField('release_date_precision', StringType(), True), StructField('total_tracks', LongType(), True), StructField('type', StringType(), True), StructField('uri', StringType(), True)]), True), StructField('artists', ArrayType(StructType([StructField('external_urls', StructType([StructField('spotify', StringType(), True)]), True), StructField('href', StringType(), True), StructField('id', StringType(), True), StructField('name', StringType(), True), StructField('type', StringType(), True), StructField('uri', StringType(), True)]), True), True), StructField('available_markets', ArrayType(StringType(), True), True), StructField('disc_number', LongType(), True), StructField('duration_ms', LongType(), True), StructField('explicit', BooleanType(), True), StructField('external_ids', StructType([StructField('isrc', StringType(), True)]), True), StructField('external_urls', StructType([StructField('spotify', StringType(), True)]), True), StructField('href', StringType(), True), StructField('id', StringType(), True), StructField('is_local', BooleanType(), True), StructField('name', StringType(), True), StructField('popularity', LongType(), True), StructField('preview_url', StringType(), True), StructField('track_number', LongType(), True), StructField('type', StringType(), True), StructField('uri', StringType(), True)]), True)])

properties = {
    "user": SPOTIFY_DB_USER,
    "password": SPOTIFY_DB_PASSWORD,
    "driver": "org.postgresql.Driver"  # PostgreSQL JDBC driver
}


normal_format = datetime.now().strftime('%Y%m%H%M')

def getSpotifyAccessToken(api):
    try:
        response = requests.get(api)
        return response.json()['access_token']
    except Exception as ex:
        raise ex

def SpotifyRecentListenList(access_token):
    try:
        SpotifyClient = Spotify(auth=access_token)
        return SpotifyClient.current_user_recently_played()
    except Exception as ex:
        raise ex
def CreateSparkSessionForS3():
    spark = SparkSession.builder.appName('Spotify ETL').getOrCreate()
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", S3_ENDPOINT)
    hadoop_conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", S3_ACCESSKEY)
    hadoop_conf.set("fs.s3a.secret.key", S3_SECRETKEY)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    return spark
def SliceExecutionTime(execution_date):
    execution_date = execution_date[:16]
    execution_date = datetime.strptime(execution_date, '%Y-%m-%dT%H:%M')
    execution_date = execution_date.strftime('%Y%m%d%H%M')
    date_dir = execution_date[0:8]
    time_dir = execution_date[8:]
    return [date_dir,time_dir]
def Ingestion(execution_date):
    logging.info(f"Starting Ingestion : {execution_date}")
    date_dir,time_dir = SliceExecutionTime(execution_date)
    s3bucket = resource(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESSKEY,
        aws_secret_access_key=S3_SECRETKEY
    )   



    access_token = getSpotifyAccessToken(SPOTIFY_API_URL)
    songslistening = SpotifyRecentListenList(access_token)
    spotifybucket =  s3bucket.Bucket(BUCKET_NAME)

    if spotifybucket not in s3bucket.buckets.all():
        logging.warning('Bucket not found hence creating')
        s3bucket.create_bucket(Bucket = BUCKET_NAME)

    for songs in songslistening['items']:
        json_str = json.dumps(songs)
        filename = f"{songs['track']['id']}{songs['played_at']}_{execution_date}"
        key = f'/raw/{date_dir}/{time_dir}/currently_playing_{filename}.json'
        logging.info(f"Processing : {key}")
        spotifybucket.put_object(
            Key=key,  
            Body=json_str, 
            ContentType='application/json' 
        )
        logging.info(f"Processing Completes : {key}")

def filterClean(execution_date):
    try:
        logging.info(f'Filtering and Cleaning Start : {execution_date}')
        spark = None
        spark = CreateSparkSessionForS3()
        date_dir,time_dir = SliceExecutionTime(execution_date)
        spotifyDF = spark.read.format('json').schema(SCHEMA).load(f's3a://{BUCKET_NAME}/raw/{date_dir}/{time_dir}/')
        spotifyDF = spotifyDF.drop('context')
        spotifyDF = spotifyDF.filter(spotifyDF['track']['duration_ms'] > 20000)
        spotifyDF.write.option('format','parquest').option('header','true').mode('overwrite').save(f's3a://{BUCKET_NAME}/filtered/{date_dir}/{time_dir}/currentlistening_{normal_format}.parquet')
        logging.info('Filtering and Cleaning Completed')
    except Exception as exp:
        logging.error(exc_info=True)
        raise exp
    finally:
        if spark is not None:
            spark.stop()

def pushToPostgres(execution_date,tablename):
    # Push To DATAWAREHOUSE/POSTGRES START
    try:
        spark = None
        spark = CreateSparkSessionForS3()
        date_dir,time_dir = SliceExecutionTime(execution_date)
        spotifyDF = spark.read.format('parquet').schema(SCHEMA).load(f's3a://{BUCKET_NAME}/filtered/{date_dir}/{time_dir}/')
        outputDF = {}
        DF_FILTERED_COL = ['played_at','track.duration_ms','track.id','track.name','track.album','track.artists']
        spotifyDF = spotifyDF.select(DF_FILTERED_COL)
        spotifyDF.cache()
        #Creating Dimension Table
        if tablename == 'spotify_track':
            spotify_track = spotifyDF.select('id','name').distinct()
            outputDF['spotify_track'] = spotify_track
        elif tablename == 'spotify_album':
            spotify_album = spotifyDF.select('album.id','album.name').distinct()
            outputDF['spotify_album'] = spotify_album
        elif tablename == 'spotify_artist':
            spotify_artist = spotifyDF.select('artists.id','artists.name').distinct()
            outputDF['spotify_artist'] = spotify_artist
        elif tablename == 'spotify_listenhistory':
            spotify_listenhistory = spotifyDF.select('id','duration_ms','played_at').distinct()
            outputDF['spotify_listenhistory'] = spotify_listenhistory
        elif tablename == 'spotify_trackalbum':
            spotify_trackalbum = spotifyDF.select(col('id').alias('trackid'),col('album.id').alias('albumid')).distinct()
            outputDF['spotify_trackalbum'] = spotify_trackalbum
        elif tablename == 'spotify_trackartist':
            spotify_trackartist = spotifyDF.select(col('id').alias('trackid'),explode(col('artists.id')).alias('artistid')).distinct()
            outputDF['spotify_trackartist'] = spotify_trackartist
        elif tablename == 'spotify_albumartist':
            spotify_albumartist = spotifyDF.select(col('album.id').alias('albumid'),explode(col('album.artists.id')).alias('artistid')).distinct()
            outputDF['spotify_albumartist'] = spotify_albumartist
        else:
            raise Exception(f'{tablename} not found')
        outputDF[tablename].write.format("jdbc").option("url", f"{POSTGRES_CONNECTION_STRING}/{SPOTIFY_DB}").option("dbtable", f"spotify.{tablename}").option("user", properties['user']).option("password", properties['password']).option("driver", properties['driver']).mode("overwrite").save()
    except Exception as exp:
        logging.error(exc_info=True)
        raise exp
    finally:
        if spark is not None:
            spark.stop()

with DAG(dag_id = 'Spotify_ETL',start_date=datetime.now()):
    
    DATASET_TO_DATABASE = ['spotify_track','spotify_album','spotify_artist','spotify_listenhistory','spotify_trackalbum','spotify_trackartist','spotify_albumartist']
    Ingestion_task = PythonOperator(task_id = 'Ingestion',python_callable=Ingestion,op_kwargs={'execution_date' : '{{ ts }}'})
    filterClean_task = PythonOperator(task_id = 'filterClean',python_callable=filterClean,op_kwargs={'execution_date' : '{{ ts }}'})
    pushToPostgres_task = [PythonOperator(task_id = 'pushToPostgres_'+dataset,python_callable=pushToPostgres,op_kwargs={'execution_date' : '{{ ts }}','tablename' : dataset}) for dataset in DATASET_TO_DATABASE]
    
    EmptyOperator(task_id = 'START') >> Ingestion_task >> filterClean_task >> pushToPostgres_task >> EmptyOperator(task_id = 'END')
