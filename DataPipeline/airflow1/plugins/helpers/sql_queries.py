class SqlQueries:
    staging_events_table_create= ("""
      CREATE TABLE IF NOT EXISTS public.staging_events (
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
      CREATE TABLE IF NOT EXISTS public.staging_songs (
        num_songs          INTEGER NOT NULL,
        artist_id          VARCHAR(25) NOT NULL,
        artist_latitude    DECIMAL,
        artist_longitude   DECIMAL,
        artist_location    VARCHAR(MAX),
        artist_name        VARCHAR(MAX),
        song_id            VARCHAR(25) NOT NULL,
        title              VARCHAR(MAX),
        duration           DECIMAL NOT NULL,
        year               INTEGER NOT NULL
      );
    """)

    songplay_table_create = ("""
      CREATE TABLE public.songplays (
	      songplay_id varchar(32) NOT NULL,
	      start_time timestamp NOT NULL,
	      userid VARCHAR NOT NULL,
	      "level" varchar(10),
	      songid varchar(256),
	      artistid varchar(256),
	      sessionid INTEGER NOT NULL,
	      location varchar(MAX),
	      user_agent varchar(256),
	      CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
      );
    """)

    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time,
                events.userid,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.sessionid,
                events.location,
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM public.staging_events
            WHERE page='NextSong') events
            LEFT JOIN public.staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM public.staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM public.staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM public.staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time),
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM public.songplays
    """)
