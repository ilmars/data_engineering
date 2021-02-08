# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE SEQUENCE IF NOT EXISTS songplay_id_seq;
    ALTER SEQUENCE songplay_id_seq RESTART WITH 1;
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int NOT NULL DEFAULT nextval('songplay_id_seq'), 
        start_time bigint, 
        user_id int, 
        level text, 
        song_id text, 
        artist_id text, 
        session_id text, 
        location text, 
        user_agent text,
        PRIMARY KEY (songplay_id)
        );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int NOT NULL, 
        first_name text, 
        last_name text, 
        gender text,  
        level text,
        PRIMARY KEY (user_id)
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id text NOT NULL, 
        title text, 
        artist_id text, 
        year int, 
        duration int,
        PRIMARY KEY (song_id)
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id text NOT NULL, 
        name text, 
        location text, 
        latitude float8, 
        longitude float8 ,
        PRIMARY KEY (artist_id)
        );
""")

time_table_create = ("""
    CREATE SEQUENCE time_id_seq;
    CREATE TABLE IF NOT EXISTS time (
        time_id int NOT NULL PRIMARY KEY DEFAULT nextval('time_id_seq'),
        start_time bigint, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
        );
    ALTER SEQUENCE time_id_seq OWNED BY time.time_id;
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level) \
                 VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) 
    DO UPDATE
        SET level  = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) \
                 VALUES (%s, %s, %s, %s, %s)
                 ON CONFLICT (song_id)
                DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                 VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                 VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, a.artist_id 
    FROM songs as s
    JOIN artists as a ON a.artist_id=s.artist_id
    WHERE s.title = %s
        AND a.name = %s
        AND s.duration = %s
    
""")
#row.song, row.artist, row.length
# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]