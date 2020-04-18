import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# CREATE AND SET SCHEMA
create_schema_query = "DROP SCHEMA IF EXISTS dwh CASCADE; CREATE SCHEMA dwh"
set_schema_query = "SET search_path TO dwh"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        id INT IDENTITY(1,1) NOT NULL,
        artist TEXT NULL,
        auth TEXT NULL,
        firstName TEXT NULL,
        gender CHAR NULL,
        itemInSession INT NULL,
        lastName TEXT NULL,
        length FLOAT NULL,
        level TEXT NULL,
        location TEXT NULL,
        method TEXT NULL,
        page TEXT NULL,
        registration DOUBLE PRECISION NULL,
        sessionId INT NULL,
        song TEXT NULL,
        status INT NULL,
        ts BIGINT NOT NULL,
        userAgent TEXT NULL,
        userId INT NULL
    )
    """)

staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs (
        id INT IDENTITY(1,1) NOT NULL,
        artist_id TEXT NOT NULL,
        artist_latitude FLOAT NULL,
        artist_location TEXT NULL,
        artist_longitude FLOAT NULL,
        artist_name TEXT NOT NULL,
        duration FLOAT NOT NULL,
        num_songs INT NOT NULL,
        song_id TEXT NOT NULL,
        title TEXT NOT NULL,
        year INT NOT NULL
    )
    """)

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(1,1) NOT NULL, 
        start_time TIMESTAMP NOT NULL distkey, 
        user_id TEXT NOT NULL, 
        level TEXT NOT NULL, 
        song_id TEXT, 
        artist_id TEXT, 
        session_id INT NOT NULL, 
        location TEXT, 
        user_agent TEXT
    )
    """)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT NOT NULL sortkey, 
        first_name TEXT, 
        last_name TEXT, 
        gender CHAR, 
        level TEXT NOT NULL
    )
    diststyle all
    """)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs (
        song_id TEXT NOT NULL sortkey, 
        title TEXT NOT NULL, 
        artist_id TEXT NOT NULL, 
        year INT NOT NULL, 
        duration FLOAT NOT NULL
    )
    diststyle all
    """)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT NOT NULL sortkey, 
        name TEXT NOT NULL, 
        location TEXT, 
        latitude FLOAT, 
        longitude FLOAT
    )
    diststyle all
    """)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP NOT NULL sortkey distkey, 
        hour INT NOT NULL, 
        day INT NOT NULL, 
        week INT NOT NULL, 
        month INT NOT NULL, 
        year INT NOT NULL, 
        weekday INT NOT NULL
    )
    """)

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {} 
    IAM_ROLE {}
    JSON {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {} 
    IAM_ROLE {}
    JSON 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + ev.ts / 1000 * interval '1 second' AS start_time,
           ev.userId,
           ev.level,
           so.song_id,
           so.artist_id,
           ev.sessionId,
           ev.location,
           ev.userAgent
    FROM staging_events ev
         JOIN staging_songs so 
              ON ev.song = so.title AND ev.artist = so.artist_name
    WHERE ev.page = 'NextSong'
    """)

user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    WITH latest AS (
        SELECT userId, MAX(ts) as ts 
        FROM staging_events 
        WHERE page = 'NextSong' AND userId IS NOT NULL
        GROUP BY userId
    )
    SELECT DISTINCT s.userId, s.firstName, s.lastName, s.gender, s.level
    FROM staging_events s
         JOIN latest ON latest.userId = s.userId
    WHERE s.ts = latest.ts;
    """)

song_table_insert = (
    """
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration 
    FROM staging_songs
    WHERE song_id IS NOT NULL;
    """)

# NOTE: While inserting into the artists table from the staging_songs table, I've realized that there are 
# some artist_ids which have more than one artist_name. Example:
#
#    SELECT DISTINCT artist_id, artist_name FROM dwh.staging_songs WHERE artist_id = 'AR065TW1187FB4C3A5';
#
#    artist_id            artist_name
#    AR065TW1187FB4C3A5   Nearly God 
#    AR065TW1187FB4C3A5   Tricky
#    AR065TW1187FB4C3A5   Tricky / The Mad Dog Reflex
#    AR065TW1187FB4C3A5   Tricky
#
# Because of that, I put MIN on every column to ensure the uniqueness of each artist_id in this table.
#
artist_table_insert = (
    """
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id, MIN(artist_name), MIN(artist_location), MIN(artist_latitude), MIN(artist_longitude)
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    GROUP BY artist_id; 
    """)


time_table_insert = (
    """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    WITH ts_converted AS (
       SELECT DISTINCT ts, timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time
       FROM staging_events
       WHERE page = 'NextSong' AND ts IS NOT NULL
    )
    SELECT start_time, 
       extract(hour from start_time) AS hour,
       extract(day from start_time) AS day,
       extract(week from start_time) AS week,
       extract(month from start_time) AS month,
       extract(year from start_time) AS year,
       extract(dow from start_time) AS weekday
    FROM ts_converted;  
    """)


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
