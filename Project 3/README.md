A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3 in a directory in JSON format. The data captures user activity on the app, as well as a directory with JSON metadata on the songs in their app. My task was to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights on what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare our results with their expected results.

AWS setup instructions:
1) Create an IAM role for REDSHIFT cluster
2) Create a default VPC ID
3) Create Security group under the VPC ID and add the security group rule for REDSHIFT cluster. The default port for REDSHIFT is 5439
4) Launch a REDSHIFT cluster and run the queries in the Query editor.

Update above information in the dwh.cfg file and then start working on below instructions.

Create staging events and staging songs tables on Redshift.
Copy json data from `s3://udacity-dend/log_data` and `s3://udacity-dend/song_data` to staging events and staging songs tables, and insert data from staging events and staging songs into fact(songplays) and dimension(users, songs, artists and time) tables.

For songs table, select song_id, title, artist_id, year, duration from staging_songs table and insert them into this table.

For artists table, select artist_id, name, location, lattitude, longitude from staging_songs table and insert them into this table.

For users table, select record from staging_events table, filter rows with `page="NextSong"` and insert user_id, first_name, last_name, gender, level in this table.

For time table, select record from staging_events table, filter rows with `page="NextSong"` and insert start_time, hour, day, week, month, year, weekday in this table.

create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.

etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

sql_queries.py is where you'll define your SQL statements, which will be imported into create_table.py and etl.py files.


sql_queries.py - Get the credentials from dwh.cfg file and CREATE statements and DROP statements for each table if it exists. Stage the events and songs using COPY commands.

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

Software skills: Python, Datawarehouse, REDSHIFT, IAM, VPC, S3,  SQL, GitHub

Error message # 1:

root@5c2499be5fb7:/home/workspace# python create_tables.py 
Traceback (most recent call last):
  File "create_tables.py", line 32, in <module>
    main()
  File "create_tables.py", line 22, in main
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
  File "/opt/conda/lib/python3.6/site-packages/psycopg2/__init__.py", line 130, in connect
    conn = _connect(dsn, connection_factory=connection_factory, **kwasync)
psycopg2.OperationalError: could not connect to server: Connection timed out
        Is the server running on host "redshift-cluster-1.c3ijhhou7n28.us-west-2.redshift.amazonaws.com" (44.225.235.58) and accepting
        TCP/IP connections on port 5439?

Solution:
In order to resolve the problem, I deleted redshift cluster and added inbound rules to default VPC (security group).

Error message # 2:
    
root@82299b736dc4:/home/workspace# python create_tables.py
Traceback (most recent call last):
  File "create_tables.py", line 32, in <module>
    main()
  File "create_tables.py", line 26, in main
    create_tables(cur, conn)
  File "create_tables.py", line 14, in create_tables
    cur.execute(query)
psycopg2.ProgrammingError: syntax error at or near "IDENTITY"
LINE 3: songplay_id IDENTITY(0,1) PRIMARY KEY, 

Solution:
In order to resolve the problem, I put songplay_id data type has int IDENTITY(0,1) PRIMARY KEY that resolved the issue.


Error message # 3:

# root@f494e112fa8d:/home/workspace# python etl.py
Traceback (most recent call last):
  File "etl.py", line 32, in <module>
    main()
  File "etl.py", line 25, in main
    load_staging_tables(cur, conn)
  File "etl.py", line 8, in load_staging_tables
    cur.execute(query)
psycopg2.InternalError: Load into table 'staging_events' failed.  Check 'stl_load_errors' system table for details.

Solution:
In order to fix the problem, I followed the instructions below.
I ran the query "select * from pg_internal."stl_load_errors" and I downloaded the output into csv format.
I opened the csv file and found the error message "Invalid timestamp format or value [YYYY-MM-DD]".
I changed the column ts data type as bigint instead of timestamp.

Error message # 4:

root@e8adea0f176e:/home/workspace# python create_tables.py
root@e8adea0f176e:/home/workspace# python etl.py
Traceback (most recent call last):
  File "etl.py", line 32, in <module>
    main()
  File "etl.py", line 26, in main
    insert_tables(cur, conn)
  File "etl.py", line 14, in insert_tables
    cur.execute(query)
psycopg2.ProgrammingError: column "user_id" does not exist in staging_events

Solution:
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                        SELECT DISTINCT user_id, first_name, last_name, gender, level
                        FROM staging_events
                        WHERE page='NextSong'
                     """)

I changed the column from user_id to userId that resolved the issue.