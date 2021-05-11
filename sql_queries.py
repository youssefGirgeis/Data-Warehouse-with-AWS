import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXITS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXITS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_session INTEGER,
        last_name VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registeration FLOAT,
        session_id INTEGER,
        song_title VARCHAR,
        status INTEGER,
        ts BIGINT,
        user_agent ,
        user_id VARCHAR
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stagnig_songs(
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_lattitude VARCHAR,
        artist_longitude VARCHAR,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT
    )
""")

# Fact Table (songplay)

# songplays - records in event data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay(
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL
        user_id VARCHAR NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR,
        FOREIGN KEY (start_time) REFERENCES time(start_time)
        FOREIGN KEY (user_id) REFERENCES users(user_id)
        FOREIGN KEY (song_id) REFERENCES songs(song_id)
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    )
""")

# Dimension Tables

# users - users in the app
# Columns: user_id, first_name, last_name, gender, level

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id VARCHAR PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR,
        level VARCHAR NOT NULL
    )
""")

# songs - songs in music database
# Columns: song_id, title, artist_id, year, duration

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year SMALLINT,
        duration FLOAT
    )
""")

# artists - artists in music database
# Columns: artist_id, name, location, lattitude, longitude

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        lattitude FLOAT,
        longitude FLOAT
    )
""")

# time - timestamps of records in songplays broken down into specific units
# Columns: start_time, hour, day, week, month, year, weekday

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday VARCHAR
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {}
    FROM {}
    iam_role {}
    json {}
    region {} 
""").format(
    'staging_events',
    config['s3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['s3']['LOG_JSONPATH'],
    'us-west-2'
)

staging_songs_copy = ("""
    COPY{}
    FROM {}
    iam_role {}
    json 'auto'
    region {}
""").format(
    'staging_songs',
    config.get('s3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    'us-west-2'
)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
