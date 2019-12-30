Sparkify offers a music streaming service.  In this project, we created a database called sparkifydb.  A schema for song play analysis was created using the song and log datasets which are defined in JSON format.  Using these datasets, I created a star schema database which has one fact table and 4 dimension tables to hold data that has been extracted from song play analysis. The fact table is songplays which hold records from log data files associated with song plays i.e. records with page “NextSong”. The fact table mainly consists of business facts and foreign keys that refers to the primary keys in the dimension tables.  The attributes of the fact tables are songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.  The dimension tables (songs, users, artists, time) contains dimensions of fact and they are joined to fact table via a foreign key.  Here are the dimension tables with attribute information.

users - users in the app
Attributes are user_id, first_name, last_name, gender, level

songs - songs in music database
Attributes are song_id, title, artist_id, year, duration

artists - artists in music database
Attributes are artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

Data: User log and song data extracted from services

Primary Package Used: Pandas

In this project, we created following 4 different scripts to create tables, inserts tables, drop tables build ETL process and ETL pipelines.  
1) sql_queries.py - CREATE statements and DROP statements for each table if it exists.  
2) create_tables.py - For creating databases and table definitions.  
3) test.ipynb - For confirming creation of tables with correct columns.  
4) etl.ipynb - Build ETL process for each table (Extracted data from a single JSON file, transformed data to correct data type, and then loaded the data into respective tables).  
5) etl.py - Build ETL pipelines for entire dataset.

I had spent quite a bit of time trying to fix the error while inserting records into time table.

Error Message: 
ProgrammingError Traceback (most recent call last) <ipython-input-18-7f5c838df038> in <module>() 1 for i, row in time_df.iterrows(): ----> 2 cur.execute(time_table_insert, list(row)) 3 conn.commit()

ProgrammingError: column "start_time" is of type bigint but expression is of type timestamp without time zone LINE 1: ...k, month, year, weekday) VALUES ('2018-11-1... ^ HINT: You will need to rewrite or cast the expression.

Solution:
In order to resolve the problem, I changed the start_time column data type as bigint and made it to PRIMARY KEY.

t = pd.to_datetime(log_df.ts, unit='ms')
year = t.dt.year
month = t.dt.month
day = t.dt.day
hour = t.dt.hour
weekday = t.dt.weekday
week = t.dt.week

time_data = (log_df.ts, hour, day, week, month, year, weekday)
column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    

