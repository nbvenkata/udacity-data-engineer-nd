Apache Cassandra Data Modeling Project

Purpose:
This project is mainly intended to create an Apache Cassandra database for querying the song play data for Sparkify (music app).  The raw data comes from CSV files and process those files to build an ETL pipeline to transform raw data into the Apache Cassandra database.

Dataset information:
The event file contains following columns 
artist, firstName of user, gender of user, item number in session, last name of user, length of the song, level (paid or free song),location of the user, sessionId, song title, userId

Process steps:
Create a list of file path to process original events CSV data files
Processing the files to create the data file csv that will be used for Apache Casssandra tables.
Create a cluster
Create and set keyspace
Create queries for new tables

Requirement #1
Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

Steps for Requirement #1
Create a table session_item_hist with desired columns (userId int, sessionId int, itemInSession int, artist text, song text, length float ) and defined PRIMARY KEYS (sessionId, itemInSession).  Primary key respresents a unique record.  All songs within itemInSession reside on the same partition i.e.SessionId. The partition keys are sessionId and itemInSession.  Every song sharing a given sessionId.

select * from session_item_hist WHERE sessionId = 338 AND itemInSession = 4

Requirement #2
Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

Steps for Requirement #2
Create a table user_session_hist with desired columns userId int, sessionId int, itemInSession int, artist text, song text, firstName text, lastName text, PRIMARY KEY ((userId, sessionId), itemInSession)). The partition keys are sessionId and itemInSession.  The clustering column (itemInSession) will sort the data in sorted ascending order.

select artist, song, firstName, lastName from user_session_hist WHERE userId = 10 AND sessionId = 182

Requirement #3
Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

Steps for Requirement #3
Created a table song_title with desired columns userId int, firstName text, lastName text, song text, PRIMARY KEY (song, userId). The partition keys are song and userId.

select userId, firstName, lastName from song_title WHERE song = 'All Hands Against His Own'

