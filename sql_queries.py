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

    artist           VARCHAR(50),
    auth             VARCHAR(20)  NOT NULL,
    first_name       VARCHAR(15),
    gender           VARCHAR(1),
    item_in_session  INTEGER      NOT NULL,
    last_name        VARCHAR(15)  NOT NULL,
    length           DECIMAL,
    level            VARCHAR(10)  NOT NULL,
    location         VARCHAR(60)  NOT NULL,
    method           VARCHAR(5)   NOT NULL, 
    page             VARCHAR(10)  NOT NULL,
    registration     BIGINT       NOT NULL,
    session_id       INTEGER      NOT NULL,
    song             VARCHAR(50),
    status           INTEGER      NOT NULL,
    ts               BIGINT       NOT NULL,
    user_agent       VARCHAR(200) NOT NULL,
    user_id          INTEGER      NOT NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    
    num_songs        INT,
    artist_id        VARCHAR(20) NOT NULL,
    artist_latitude  DECIMAL,
    artist_longitude DECIMAL,
    artist_location  VARCHAR(50),
    artist_name      VARCHAR(50) NOT NULL,
    song_id          VARCHAR(20) NOT NULL,
    title            VARCHAR(50) NOT NULL,
    duration         DECIMAL     NOT NULL,
    YEAR             INT         NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table(

    songplay_id      INTEGER     IDENTITY(0,1) PRIMARY KEY SORTKEY,
    start_time       TIMESTAMP   NOT NULL,
    user_id          INTEGER     NOT NULL,
    level            VARCHAR(10) NOT NULL,
    song_id          INTEGER     NOT NULL,
    artist_id        INTEGER     NOT NULL,
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

    song_id          INTEGER     PRIMARY KEY SORTKEY,
    title            VARCHAR(50) NOT NULL,
    artist_id        INTEGER,
    year             INTEGER     NOT NULL DISTKEY,
    duration         DECIMAL     NOT NULL
);    
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table(

    artist_id        INTEGER     PRIMARY KEY SORTKEY, 
    name             VARCHAR(50) NOT NULL,
    location         VARCHAR(50) NOT NULL,
    lattitude        DECIMAL,
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
json 'auto
""").format(config.get('IAM_ROLE','ARN'))

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
