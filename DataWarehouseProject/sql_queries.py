import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
def drop_table_statement(table_name):
    return "DROP TABLE IF EXISTS {};".format(table_name)

staging_events_table_drop = drop_table_statement("staging_events")
staging_songs_table_drop = drop_table_statement("staging_songs")
songplay_table_drop = drop_table_statement("songplay")
user_table_drop = drop_table_statement("user")
song_table_drop = drop_table_statement("song")
artist_table_drop = drop_table_statement("artist")
time_table_drop = drop_table_statement("time")

# CREATE TABLES

staging_events_table_create= ("""
  CREATE TABLE staging_events (
    event_id        INTERVAL(0, 1)
    artist          VARCHAR(100),
    auth            VARCHAR(25),
    firstName       VARCHAR(15),
    gender          VARCHAR(1),
    itemInSession   INTEGER NOT NULL,
    lastName        VARCHAR(15),
    length          DECIMAL,
    level           VARCHAR(10),
    location        VARCHAR(100),
    method          VARCHAR(6),
    page            VARCHAR(25),
    registration    DECIMAL,
    sessionId       INTEGER NOT NULL,
    song            VARCHAR(100),
    status          INTEGER NOT NULL,
    ts              INTEGER NOT NULL,
    userAgent       VARCHAR(100),
    userId          INTEGER NOT NULL
  );
""")

staging_songs_table_create = ("""
  CREATE TABLE staging_songs (
    artist_id          VARCHAR(25) NOT NULL,
    artist_latitude    DECIMAL,
    artist_longitude   DECIMAL,
    artist_location    VARCHAR(50),
    artist_name        VARCHAR(100),
    song_id            VARCHAR(25) NOT NULL,
    title              VARCHAR(100),
    duration           DECIMAL,
    year               INTEGER
  );
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

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
