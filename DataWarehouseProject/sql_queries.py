# DROP TABLES
def drop_table_statement(table_name, cascade=False):
    if cascade:
        return "DROP TABLE IF EXISTS {} CASCADE".format(table_name)
    return "DROP TABLE IF EXISTS {};".format(table_name)

def get_drop_table_queries():
    staging_events_table_drop = drop_table_statement("staging_events")
    staging_songs_table_drop = drop_table_statement("staging_songs")
    songplay_table_drop = drop_table_statement("songplay", cascade=True)
    user_table_drop = drop_table_statement("users")
    song_table_drop = drop_table_statement("song")
    artist_table_drop = drop_table_statement("artist")
    time_table_drop = drop_table_statement("time")
    return [staging_events_table_drop, staging_songs_table_drop,
            songplay_table_drop, user_table_drop, song_table_drop,
            artist_table_drop, time_table_drop]

# CREATE TABLES
def get_create_table_queries():
    staging_events_table_create= ("""
      CREATE TABLE staging_events (
        event_id        BIGINT IDENTITY(0, 1),
        artist          VARCHAR,
        auth            VARCHAR(25),
        firstName       VARCHAR(15),
        gender          VARCHAR(1),
        itemInSession   INTEGER NOT NULL,
        lastName        VARCHAR(15),
        length          DECIMAL,
        level           VARCHAR(10),
        location        VARCHAR,
        method          VARCHAR(6),
        page            VARCHAR(25),
        registration    DECIMAL,
        sessionId       INTEGER NOT NULL,
        song            VARCHAR,
        status          INTEGER NOT NULL,
        ts              BIGINT NOT NULL,
        userAgent       VARCHAR,
        userId          VARCHAR NOT NULL
      );
    """)

    staging_songs_table_create = ("""
      CREATE TABLE staging_songs (
        num_songs          INTEGER NOT NULL,
        artist_id          VARCHAR(25) NOT NULL,
        artist_latitude    DECIMAL,
        artist_longitude   DECIMAL,
        artist_location    VARCHAR,
        artist_name        VARCHAR,
        song_id            VARCHAR(25) NOT NULL,
        title              VARCHAR,
        duration           DECIMAL NOT NULL,
        year               INTEGER NOT NULL
      );
    """)

    songplay_table_create = ("""
      CREATE TABLE songplay (
        songplay_id   BIGINT IDENTITY(0, 1),
        start_time    TIMESTAMP NOT NULL sortkey,
        user_id       INTEGER NOT NULL,
        level         VARCHAR(10),
        song_id       VARCHAR(25) NOT NULL distkey,
        artist_id     VARCHAR(25) NOT NULL,
        session_id    INTEGER NOT NULL,
        location      VARCHAR,
        user_agent    VARCHAR
      );
    """)

    user_table_create = ("""
      CREATE TABLE users (
        user_id       BIGINT NOT NULL sortkey,
        first_name    VARCHAR(15),
        last_name     VARCHAR(15),
        gender        VARCHAR(1),
        level         VARCHAR(10)
      ) diststyle all;
    """)

    song_table_create = ("""
      CREATE TABLE song (
        song_id       VARCHAR(25) NOT NULL sortkey distkey,
        title         VARCHAR,
        artist_id     VARCHAR(25) NOT NULL,
        year          INTEGER NOT NULL,
        duration      DECIMAL NOT NULL
      );
    """)

    artist_table_create = ("""
      CREATE TABLE artist (
        artist_id     VARCHAR(25) NOT NULL sortkey,
        name          VARCHAR,
        location      VARCHAR,
        latitude      DECIMAL,
        longitude     DECIMAL
      ) diststyle all;
    """)

    time_table_create = ("""
      CREATE TABLE time (
        start_time    TIMESTAMP NOT NULL sortkey,
        hour          INTEGER NOT NULL,
        day           INTEGER NOT NULL,
        week          INTEGER NOT NULL,
        month         INTEGER NOT NULL,
        year          INTEGER NOT NULL,
        weekday       VARCHAR(1) NOT NULL
      ) diststyle all;
    """)

    return [staging_events_table_create, staging_songs_table_create,
            songplay_table_create, user_table_create, song_table_create,
            artist_table_create, time_table_create]

# STAGING TABLES
def get_copy_table_queries(config):
    staging_events_copy = ("""
      COPY staging_events FROM '{events_path}'
      CREDENTIALS 'aws_iam_role={role_arn}' compupdate off
      FORMAT as json '{json_format_file}'
      REGION 'us-west-2';
    """).format(events_path=config.get('S3', 'log_data'),
                role_arn=config.get('IAM_ROLE', 'role_arn'),
                json_format_file=config.get('S3', 'log_jsonpath'))

    staging_songs_copy = ("""
      COPY staging_songs FROM '{song_path}'
      CREDENTIALS 'aws_iam_role={role_arn}' compupdate off
      FORMAT as json 'auto'
      REGION 'us-west-2';
    """).format(song_path=config.get('S3', 'song_data'),
                role_arn=config.get('IAM_ROLE', 'role_arn'))

    return [staging_events_copy, staging_songs_copy]

# FINAL TABLES
def get_insert_table_queries(config):
    songplay_table_insert = ("""
      INSERT INTO songplay (start_time, user_id, level, song_id, artist_id,
                            session_id, location, user_agent)
      SELECT
        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second') AS start_time,
        CAST(e.userId AS INTEGER) AS user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId AS session_id,
        e.location,
        e.userAgent AS user_agent
      FROM
        staging_events AS e
      INNER JOIN
        staging_songs AS s
      ON
        e.song = s.title AND e.artist = s.artist_name
      WHERE
        e.page = 'NextSong'
      ORDER BY
        e.ts ASC
      ;
    """)

    user_table_insert = ("""
      INSERT INTO users (user_id, first_name, last_name, gender, level)
      SELECT
        DISTINCT CAST(userId AS INTEGER) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
      FROM
        staging_events
      ;
    """)

    song_table_insert = ("""
      INSERT INTO song (song_id, title, artist_id, year, duration)
      SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
      FROM
        staging_songs
      ;
    """)

    artist_table_insert = ("""
      INSERT INTO artist (artist_id, name, location, latitude, longitude)
      SELECT
        DISTINCT artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
      FROM
        staging_songs
      ;
    """)

    time_table_insert = ("""
      INSERT INTO time (start_time, hour, day, week, month, year, weekday)
      SELECT
        ts AS start_time,
        EXTRACT(hour FROM ts) AS hour,
        EXTRACT(day FROM ts) AS day,
        EXTRACT(week FROM ts) AS week,
        EXTRACT(month FROM ts) AS month,
        EXTRACT(year FROM ts) AS year,
        EXTRACT(weekday FROM ts) AS weekday
      FROM (
        SELECT
          DISTINCT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') AS ts
        FROM
          staging_events)
      ;
    """)

    return [songplay_table_insert, user_table_insert, song_table_insert,
            artist_table_insert, time_table_insert]
