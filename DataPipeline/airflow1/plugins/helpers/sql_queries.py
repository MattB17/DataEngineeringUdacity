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
      CREATE TABLE IF NOT EXISTS public.songplay (
        songplay_id   VARCHAR NOT NULL,
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

    songplay_table_insert = ("""
      SELECT
        md5(e.sessionId || TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) AS songplay_id,
        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second') AS start_time,
        CAST(e.userId AS INTEGER) AS user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId AS session_id,
        e.location,
        e.userAgent AS user_agent
      FROM
        public.staging_events AS e
      INNER JOIN
        public.staging_songs AS s
      ON
        e.song = s.title AND e.artist = s.artist_name
      WHERE
        e.page = 'NextSong'
      ORDER BY
        e.ts ASC
    """)

    user_table_create = ("""
      CREATE TABLE IF NOT EXISTS public.user (
        user_id       BIGINT NOT NULL sortkey,
        first_name    VARCHAR(15),
        last_name     VARCHAR(15),
        gender        VARCHAR(1),
        level         VARCHAR(10),
	      CONSTRAINT users_pkey PRIMARY KEY (user_id)
      );
    """)

    user_table_insert = ("""
      SELECT
        DISTINCT CAST(userId AS INTEGER) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
      FROM
        public.staging_events
      WHERE
        userId != ''
        AND userId != ' '
        AND page = 'NextSong'
    """)

    song_table_create = ("""
      CREATE TABLE IF NOT EXISTS public.song (
        song_id       VARCHAR(25) NOT NULL sortkey distkey,
        title         VARCHAR(MAX),
        artist_id     VARCHAR(25) NOT NULL,
        year          INTEGER NOT NULL,
        duration      DECIMAL NOT NULL,
        CONSTRAINT songs_pkey PRIMARY KEY (song_id)
      );
    """)

    song_table_insert = ("""
      SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
      FROM
        public.staging_songs
    """)

    artist_table_create = ("""
      CREATE TABLE IF NOT EXISTS public.artist (
        artist_id     VARCHAR(25) NOT NULL sortkey,
        name          VARCHAR(MAX),
        location      VARCHAR(MAX),
        latitude      DECIMAL,
        longitude     DECIMAL,
        CONSTRAINT artists_pkey PRIMARY KEY (artist_id)
      );
    """)

    artist_table_insert = ("""
      SELECT
        DISTINCT artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
      FROM
        public.staging_songs
    """)

    time_table_create = ("""
      CREATE TABLE public.time (
        start_time    TIMESTAMP NOT NULL sortkey,
        hour          INTEGER NOT NULL,
        day           INTEGER NOT NULL,
        week          INTEGER NOT NULL,
        month         INTEGER NOT NULL,
        year          INTEGER NOT NULL,
        weekday       VARCHAR(1) NOT NULL,
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
      );
    """)

    time_table_insert = ("""
      SELECT
        start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(weekday FROM start_time) AS weekday
      FROM
        public.songplay
    """)
