import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

#artist,auth,firstName,gender,ItemInSession,lastName,length,level,location,method,page,
#registration,sessionId,song,status,ts,userAgent,userId
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist text, 
auth text, 
firstName text, 
gender text, 
ItemInSession int,
lastName text,
length float,
level text,
location text,
method text,
page text,
registration text, 
sessionId int,
song text,
status int,
ts bigint, 
userAgent text, 
userId int
)
""")

#num_songs,artist_id,artist_latitude,artist_logitude,artist_location,artist_name,song_id,title,duration,year 
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs( 
num_songs text, 
artist_id text, 
artist_latitude text, 
artist_logitude text, 
artist_location text, 
artist_name text, 
song_id text, 
title text, 
duration text,
year int 
)
""")

#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id int IDENTITY(0,1) PRIMARY KEY, 
start_time bigint NOT NULL, 
user_id int NOT NULL, 
level text,
song_id text,
artist_id text, 
session_id int, 
location text, 
user_agent text
)
""")

#user_id, first_name, last_name, gender, level
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int NOT NULL PRIMARY KEY, 
first_name text, 
last_name text,
gender char, 
level text
)
""")

#song_id, title, artist_id, year, duration
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id text NOT NULL PRIMARY KEY, 
title text, 
artist_id text, 
year int,
duration numeric (10,4)
)
""")

#artist_id, name, location, latitude, longitude
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id text NOT NULL PRIMARY KEY,
name text,
location text, 
latitude numeric (10, 2),
longitude numeric (10, 2)
)                  
""")

#start_time, hour, day, week, month, year, weekday
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp NOT NULL PRIMARY KEY,
hour int,
day int, 
week int,
month int, 
year int, 
weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events
from {} 
iam_role {}
json {} 
""").format(config['S3'] ['LOG_DATA'], config['IAM_ROLE'] ['ARN'], config['S3'] ['LOG_JSONPATH'])


staging_songs_copy = ("""
copy staging_songs
from {} 
iam_role {}
json 'auto'
""").format(config['S3'] ['SONG_DATA'], config['IAM_ROLE'] ['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id,  level, song_id, 
                            artist_id,session_id,location,user_agent) 
                            SELECT events.ts, events.userId, events.level,
                            songs.song_id, songs.artist_id, events.sessionId,
                            events.location, events.userAgent 
                            FROM
                            (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, * FROM
                             staging_events WHERE
                             page='NextSong') events LEFT JOIN
                             staging_songs songs ON 
                             events.song = songs.title AND 
                             events.artist = songs.artist_name AND
                             events.length = songs.duration
                         """)


user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                        SELECT DISTINCT userId, firstName, lastName, gender, level
                        FROM staging_events
                        WHERE page='NextSong'
                     """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs
                     """)


artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_logitude
                          FROM staging_songs
                       """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                        SELECT DISTINCT  start_time
                       ,EXTRACT(HOUR FROM start_time) As hour
                       ,EXTRACT(DAY FROM start_time) As day
                       ,EXTRACT(WEEK FROM start_time) As week
                       ,EXTRACT(MONTH FROM start_time) As month
                       ,EXTRACT(YEAR FROM start_time) As year
                       ,EXTRACT(DOW FROM start_time) As weekday
                       FROM 
                       (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, * FROM
                        staging_events WHERE
                        page='NextSong')
                     """) 

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
