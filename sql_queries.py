import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
## Needed?
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "drop table if exists events_staging"
staging_songs_table_drop = "drop table if exists songs_staging"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists times"


# CREATE TABLES
staging_events_table_create= ("""
    create table if not exists events_staging (
        se_id integer IDENTITY(0,1),--?
        artist text,
        auth varchar not null,
        first_name varchar,
        gender char (1),
        item_in_session int not null,
        last_name varchar,
        length numeric,
        level varchar not null,
        location varchar,
        method varchar not null,
        page varchar not null,
        registration numeric,
        session_id int not null,
        song varchar,
        status int not null,
        ts numeric not null,
        user_agent varchar,
        user_id int
    )
""")

staging_songs_table_create = ("""
    create table if not exists songs_staging (
        se_id integer IDENTITY(0,1),--?
        num_songs int not null,
        artist_id varchar not null,
        artist_latitude varchar,
        artist_longitude varchar,
        artist_location varchar,
        artist_name varchar not null,
        song_id varchar not null,
        title varchar not null,
        duration numeric not null,
        year int not null
    )
""")

songplay_table_create = ("""
    create table if not exists songplays (
        --songplay_id int identity(0, 1) primary key,
        start_time timestamp not null,
        user_id int not null,
        level varchar not null,
        song_id varchar,
        artist_id varchar,
        session_id int not null,
        location varchar,
        user_agent varchar not null,
        PRIMARY KEY (song_id, artist_id, start_time)
    )
""")

user_table_create = ("""
    create table if not exists users (
        user_id int primary key,
        first_name varchar not null,
        last_name varchar not null,
        gender char (1) not null,
        level varchar not null
    )
""")

song_table_create = ("""
    create table if not exists songs (
        song_id varchar primary key,
        title varchar not null,
        artist_id varchar not null,
        year int not null,
        duration numeric not null
    )
""")

artist_table_create = ("""
    create table if not exists artists (
        artist_id varchar primary key,
        artist_name text not null,
        artist_location varchar,
        artist_longitude numeric,
        artist_latitude numeric
    )
""")

time_table_create = ("""
    create table if not exists times (
        start_time timestamp primary key,
        hour int not null,
        day int not null,
        week int not null,
        month int not null,
        year int not null,
        weekday int not null
    )
""")

# STAGING TABLES

staging_events_copy = ("""COPY events_staging
                          FROM {}
                          CREDENTIALS 'aws_iam_role={}'
                          REGION 'us-west-2'
                          COMPUPDATE OFF
                          STATUPDATE OFF 
                          JSON AS {}
                       """).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""COPY songs_staging
                         FROM {}
                         CREDENTIALS 'aws_iam_role={}'
                         REGION 'us-west-2'
                         COMPUPDATE OFF
                         STATUPDATE OFF 
                         JSON as 'auto'
                      """).format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES
user_table_insert = ("""
    INSERT INTO users
     (user_id, first_name, 
      last_name, gender, level)  
    SELECT
     e.user_id,
     e.first_name,
     e.last_name,
     e.gender,
     coalesce(eo.level, e.level)
    FROM
    (
    SELECT
     DISTINCT
     eo.user_id, eo.first_name, eo.last_name, eo.gender, eo.level
    FROM
     events_staging eo
    WHERE user_id IS NOT NULL
    AND eo.level = 'free') e
    LEFT JOIN
    (SELECT
     DISTINCT
     eo.user_id, eo.first_name, eo.last_name, eo.gender, eo.level
    FROM
     events_staging eo
    WHERE user_id IS NOT NULL
    AND eo.level <> 'free') eo ON(e.user_id = eo.user_id)
""")

artist_table_insert = ("""
    INSERT INTO artists
     (artist_id, artist_name,
      artist_location, artist_latitude, artist_longitude)
    SELECT
    DISTINCT
     artist_id,
     artist_name as name,
     artist_location as location,
     artist_latitude as latitude,
     artist_longitude as longitude
    FROM
     songs_staging
""")

song_table_insert = ("""
    INSERT INTO songs
    (song_id, title,
     artist_id, year, duration)  
    SELECT
    DISTINCT
     song_id,
     title,
     artist_id,
     year,
     duration
    FROM 
     songs_staging
    WHERE song_id IS NOT NULL
""")


time_table_insert = ("""
    INSERT INTO times 
    (start_time, hour, day,
     week, month, year, weekday)
    SELECT
     ti.start_time,
     extract(hour from ti.start_time) as hour,
     extract(day from ti.start_time) as day,
     extract(week from ti.start_time) as week,
     extract(month from ti.start_time) as month,
     extract(year from ti.start_time) as year,
     extract(weekday from ti.start_time) as weekday
    FROM 
     (
       SELECT 
       DISTINCT
        timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time
       FROM 
        events_staging
       WHERE
        page = 'NextSong'
    ) ti
""")

songplay_table_insert = ("""
    INSERT INTO songplays 
    (start_time, user_id, level, song_id, artist_id,
     session_id, location, user_agent)
    SELECT
    DISTINCT
     timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time,
     e.user_id as user_id,
     e.level,
     s.song_id,
     s.artist_id,
     e.session_id as session_id,
     e.location,
     e.user_agent as user_agent
    FROM
     events_staging e, 
     songs_staging s
    WHERE
     e.page = 'NextSong'
     AND e.user_id IS NOT NULL
     AND e.song = s.title and e.artist = s.artist_name
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
