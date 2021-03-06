import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('credentials/dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events( 

    artist           VARCHAR(200),
    auth             VARCHAR(20)  NOT NULL,
    first_name       VARCHAR(15),
    gender           VARCHAR(1),
    item_in_session  INTEGER      NOT NULL,
    last_name        VARCHAR(15),
    length           DECIMAL,
    level            VARCHAR(10)  NOT NULL,
    location         VARCHAR(60),
    method           VARCHAR(5)   NOT NULL, 
    page             VARCHAR(50),
    registration     VARCHAR(13),
    session_id       INTEGER      NOT NULL,
    song             VARCHAR(200),
    status           INTEGER      NOT NULL,
    ts               BIGINT       NOT NULL,
    user_agent       VARCHAR(200),
    user_id          INTEGER 
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    
    num_songs        INT,
    artist_id        VARCHAR(20) NOT NULL,
    artist_latitude  DECIMAL,
    artist_longitude DECIMAL,
    artist_location  VARCHAR(200),
    artist_name      VARCHAR(200),
    song_id          VARCHAR(20) NOT NULL,
    title            VARCHAR(200),
    duration         DECIMAL     NOT NULL,
    year             INTEGER     NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table(

    user_id          INTEGER     PRIMARY KEY SORTKEY,
    first_name       VARCHAR(15),
    last_name        VARCHAR(15),
    gender           VARCHAR(1),
    level            VARCHAR(10)
);    
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table(

    song_id          VARCHAR(20) PRIMARY KEY SORTKEY,
    title            VARCHAR(200),
    artist_id        VARCHAR(20),
    year             INTEGER DISTKEY,
    duration         DECIMAL
);    
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table(

    artist_id        VARCHAR(20) PRIMARY KEY SORTKEY, 
    name             VARCHAR(200),
    location         VARCHAR(200),
    latitude         DECIMAL,
    longitude        DECIMAL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(

    start_time       DATETIME    PRIMARY KEY SORTKEY,
    hour             INTEGER     NOT NULL,
    day              INTEGER     NOT NULL,
    week             INTEGER     NOT NULL,
    month            INTEGER     NOT NULL,
    year             INTEGER     NOT NULL,
    weekday          INTEGER     NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table(

    songplay_id      INTEGER     IDENTITY(0,1) PRIMARY KEY SORTKEY,
    start_time       TIMESTAMP   NOT NULL REFERENCES time_table(start_time),
    user_id          INTEGER     NOT NULL REFERENCES user_table(user_id),
    level            VARCHAR(10) NOT NULL,
    song_id          VARCHAR(20) NOT NULL REFERENCES song_table(song_id),
    artist_id        VARCHAR(20) NOT NULL REFERENCES artist_table(artist_id),
    session_id       INTEGER     NOT NULL DISTKEY,
    location         VARCHAR(60),
    user_agent       VARCHAR(200)
);
""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
REGION {}
json {}
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE','ARN'),
            config.get('S3','REGION'),
            config.get('S3','LOG_JSONPATH')
            )

staging_songs_copy = ("""
COPY staging_songs
FROM {} 
IAM_ROLE {}
REGION {}
json 'auto'
""").format(config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE','ARN'),
            config.get('S3','REGION')
            )

# FINAL TABLES


user_table_insert = ("""


INSERT INTO user_table( user_id,
                        first_name,
                        last_name,
                        gender,
                        level)

WITH 
unique_users AS(
    SELECT user_id, first_name, last_name, gender, level, 
    RANK() OVER(PARTITION BY user_id ORDER BY ts DESC) AS times_logged
    FROM staging_events WHERE user_id IS NOT NULL
)
SELECT  user_id    AS user_id,
        first_name AS first_name,
        last_name  AS last_name,
        gender     AS gender,
        level      AS level                               
FROM unique_users
WHERE times_logged = 1
;
""")

song_table_insert = ("""
INSERT INTO song_table(     song_id,
                            title,
                            artist_id,
                            year,       
                            duration)
SELECT  DISTINCT
        song_id AS song_id,
        title AS title,
        artist_id AS artist_id,
        year AS year,
        duration AS duration
FROM staging_songs
WHERE song_id IS NOT NULL
;
""")

artist_table_insert = ("""
INSERT INTO artist_table(   artist_id,
                            name,
                            location,
                            latitude,
                            longitude)
WITH
unique_artists AS(
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude, 
    RANK() OVER(PARTITION BY artist_id ORDER BY song_id) AS times_logged
    FROM staging_songs WHERE artist_id IS NOT NULL
)
SELECT  artist_id AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude::DECIMAL AS latitude,
        artist_longitude::DECIMAL AS longitude
FROM unique_artists
WHERE times_logged = 1 
;
""")

songplay_table_insert = ("""
INSERT INTO songplay_table(     start_time,
                                user_id   ,
                                level     ,
                                song_id   ,
                                artist_id ,
                                session_id,
                                location  ,
                                user_agent)
                                
SELECT  TIMESTAMP 'epoch'+se.ts*INTERVAL '1 second' AS start_time,
        se.user_id AS user_id,
        se.level AS level,
        ss.song_id          AS song_id,
        ss.artist_id        AS artist_id,
        se.session_id       AS session_id,
        se.location AS location, 
        se.user_agent AS user_agent
        
        
FROM staging_events       AS se
LEFT JOIN staging_songs   AS ss ON se.song=ss.title 
                                AND se.artist=ss.artist_name
                                AND se.length=ss.duration
                                
WHERE se.ts IS NOT NULL
AND se.user_id IS NOT NULL
AND se.level IS NOT NULL
AND ss.song_id IS NOT NULL
AND ss.artist_id IS NOT NULL 
AND session_id IS NOT NULL
;
""")

time_table_insert = ("""
INSERT INTO time_table( start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
SELECT  DISTINCT 
        start_time AS start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEK FROM start_time) AS week, 
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        EXTRACT(DOW FROM start_time) AS weekday
FROM songplay_table
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]
