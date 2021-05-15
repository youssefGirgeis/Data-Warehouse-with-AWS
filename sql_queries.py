import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# staging table 1
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist_name VARCHAR,
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
        ts TIMESTAMP,
        user_agent VARCHAR,
        user_id VARCHAR
    )
""")

# staging table 2
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_lattitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year SMALLINT
    )
""")

# Fact Table (songplay)

# songplays - records in event data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id VARCHAR NOT NULL DISTKEY,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    )
    diststyle key;
""")

# Dimension Tables

# users - users in the app
# Columns: user_id, first_name, last_name, gender, level

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id VARCHAR PRIMARY KEY SORTKEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR,
        level VARCHAR NOT NULL
    )
    diststyle all;
""")

# songs - songs in music database
# Columns: song_id, title, artist_id, year, duration

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR PRIMARY KEY SORTKEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL DISTKEY,
        year SMALLINT,
        duration FLOAT
    )
    diststyle key;
""")

# artists - artists in music database
# Columns: artist_id, name, location, lattitude, longitude

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        lattitude FLOAT,
        longitude FLOAT
    )
    diststyle all;
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
        year SMALLINT DISTKEY,
        weekday SMALLINT
    )
    diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {}
    FROM {}
    iam_role {}
    json {}
    TIMEFORMAT as 'epochmillisecs'
""").format(
    'staging_events',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH']
)

staging_songs_copy = ("""
    COPY {}
    FROM {}
    iam_role {}
    json 'auto'
""").format(
    'staging_songs',
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        se.ts,
        se.user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.session_id,
        se.location,
        se.user_agent
    FROM staging_events se
    JOIN staging_songs ss
    ON se.song_title = ss.title
        AND se.artist_name = ss.artist_name
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT
        DISTINCT user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs      
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_lattitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT
        DISTINCT ts,
        EXTRACT(hour FROM ts),
        EXTRACT(day FROM ts),
        EXTRACT(week FROM ts),
        EXTRACT(month FROM ts),
        EXTRACT(year FROM ts),
        EXTRACT(weekday FROM ts)
    FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
