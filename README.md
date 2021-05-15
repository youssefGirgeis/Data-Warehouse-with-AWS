# Date Warehouse on AWS

## Project Introduction
<p>A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.</p>

<p>In this project, I built an ETL pipeline for database hosted on Amazon Redshift. Data was loaded from S3 to staging tables on Redshift. Then, to load the data from these staging tables into the analytics tables, I wrote sql statments to create  those tables and insert the data into them. </p>

## Database Schema: Star
**Staging Table**
1- staging_songs - information about songs and artists
2- staging_events - users activities (which song are listening, etc.. )

**Fact tables:**
Table Name: songplays - records in event data associated with song plays i.e. records with page NextSong
Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agen

**Dimension tables**
1- users: users in the app
Columns: user_id, first_name, last_name, gender, level

2- songs: songs in music database
Columns song_id, title, artist_id, year, duration

3- artists: artists in music database
Columns artist_id, name, location, lattitude, longitude

4- time: timestamps of records in songplays broken down into specific units
Columns: start_time, hour, day, week, month, year, weekday

![ERD](ERD.PNG)

## Project Structure
**The project template includes four files:**

**1- create_table.py**: creates the fact and dimension tables for the star schema in Redshift.
**2- etl.py**: loads data from S3 into staging tables on Redshift and then process that data into the fact and dimension tables on Redshift.
**3- sql_queries.py:** defins the SQL statements, which will be imported into the two other files above.
**4- dwh.cfg**: Configuration file that contains information about Redshift, IAM, and S3

## ETL Pipeline
1- create the staging and analytics tables

2- Load the data from s3 to the staging tables on Redshift

3- Insert data into the analytics tables (facts and dimensions) from staging tables.

## AWS Configurations and Setup 
1- Created IAM user to access Redshift (full access)

2- Created IAM Role to allow Redshift to access S3 (read access)

3- created A RedShift Cluster

4- Populated the above info in the config file

## How to Run
**Step 1**: run create_tables.py to create the staging and analytics tables
**Step 2** run etl.py to load data from S3 to staging tables on Redshift and to load data from staging tables to analytics tables on Redshift

