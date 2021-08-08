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
    registration     BIGINT,
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

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table(

    songplay_id      INTEGER     IDENTITY(0,1) PRIMARY KEY SORTKEY,
    start_time       TIMESTAMP   NOT NULL,
    user_id          INTEGER     NOT NULL,
    level            VARCHAR(10) NOT NULL,
    song_id          VARCHAR(20) NOT NULL,
    artist_id        VARCHAR(20) NOT NULL,
    session_id       INTEGER     NOT NULL DISTKEY,
    location         VARCHAR(60),
    user_agent       VARCHAR(200)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table(

    user_id          INTEGER     PRIMARY KEY SORTKEY,
    first_name       VARCHAR(15) NOT NULL,
    last_name        VARCHAR(15) NOT NULL,
    gender           VARCHAR(2)  NOT NULL,
    level            VARCHAR(10) NOT NULL
);    
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table(

    song_id          VARCHAR(20) PRIMARY KEY SORTKEY,
    title            VARCHAR(200) NOT NULL,
    artist_id        VARCHAR(20),
    year             INTEGER     NOT NULL DISTKEY,
    duration         DECIMAL     NOT NULL
);    
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table(

    artist_id        VARCHAR(20) PRIMARY KEY SORTKEY, 
    name             VARCHAR(200) NOT NULL,
    location         VARCHAR(50) NOT NULL,
    latitude        DECIMAL,
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

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM 's3://udacity-dend/log_data'
IAM_ROLE {}
REGION 'us-west-2'
json 's3://udacity-dend/log_json_path.json'
""").format(config.get('IAM_ROLE','ARN'))

staging_songs_copy = ("""
COPY staging_songs
FROM 's3://udacity-dend/song_data' 
IAM_ROLE {}
REGION 'us-west-2'
json 'auto'
""").format(config.get('IAM_ROLE','ARN'))

# FINAL TABLES

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
                                AND se.artist=ss.artist_name;
""")

user_table_insert = ("""
INSERT INTO user_table( user_id,
                        first_name,
                        last_name,
                        gender,
                        level)
SELECT user_id     AS user_id,
       first_name AS first_name,
       last_name  AS last_name,
       gender     AS gender,
       level      AS level                               
FROM staging_events;
""")

song_table_insert = ("""
INSERT INTO song_table(     song_id,
                            title,
                            artist_id,
                            year,       
                            duration)
SELECT  song_id AS song_id,
        title AS title,
        artist_id AS artist_id,
        year AS year,
        duration AS duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artist_table(   artist_id,
                            name,
                            location,
                            latitude,
                            longitude)
SELECT  artist_id AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude::DECIMAL AS latitude,
        artist_longitude::DECIMAL AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time_table( start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
SELECT  start_time AS start_time,
        EXTRACT(HOUR FROM start_time) AS hour,
        EXTRACT(DAY FROM start_time) AS day,
        EXTRACT(WEEK FROM start_time) AS week, 
        EXTRACT(MONTH FROM start_time) AS month,
        EXTRACT(YEAR FROM start_time) AS year,
        EXTRACT(DOW FROM start_time) AS weekday
FROM songplay_table
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
