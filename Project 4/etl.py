import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read song data and process it and save to provided output location
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
    # Insert records into songs and artists tables using data from song files.
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()
        
    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Read song data and process it and save to provided output location
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
    # """Insert records into users, time and songplays tables using data from log files. """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
        
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
     
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts)) 
    
    # create datetime column from original timestamp column
    datetime_from_timestamp = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000.0)))
    df = df.withColumn("datetime", datetime_from_timestamp(df.ts))

    # extract columns to create time table
    time_table = df.select(
        'timestamp',
        hour('datetime').alias('hour'),
        date_format('datetime', 'd').alias('day'),
        date_format('datetime', 'w').alias('week'),
        date_format('datetime', 'MM').alias('month'),
        date_format('datetime', 'y').alias('year'),
        date_format('datetime', 'F').alias('weekday')
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song-data/*/*/*/TRA*.json").dropDuplicates()

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(df.song == song_df.title) & (df.artist == song_df.artist_name) & 
                             (df.length == song_df.duration), "left_outer")\
                        .select(
                         df.timestamp,
                         col("userId").alias('user_id'),
                         col("level").alias('Level'), 
                         col("song_ID").alias('song_Id'),
                         col("artist_ID").alias('artist_Id'),
                         col("sessionId").alias('session_Id'),
                         df.location,
                         col("userAgent").alias('user_Agent'),
                         year('datetime').alias('year'),
                         month('datetime').alias('month')
                              )
  
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'songplays'), 'overwrite')


def main():
    
    # read config file
    config =  configparser.ConfigParser()
    config.read('dl.cfg')
    
    # set access keys
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    spark = create_spark_session()
    
    # input_data is our udacity data
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://naveen-bucket-dend1"
        
    # process_song_data function has spark object, input_data and out_put data
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
