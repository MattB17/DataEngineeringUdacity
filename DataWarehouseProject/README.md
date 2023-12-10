# Data Warehouse Project
The purpose of this project is to create an ETL pipeline and data warehouse for a fictitious company, Sparkify. Sparkify wants to create a data warehouse optimized for analysis on song play info.

### Raw Data
The data warehouse will be built from 3 files in s3:
* song data `s3://udacity-dend/song_data`
* log data `s3://udacity-dend/log_data`
* metadata used to correctly load the song data `s3://udacity-dend/log_json_path.json`

The song dataset has each file in JSON format. The files contain metadata about a song and the artist of that song. The files are partitioned by the first 3 letters of each song's track ID.

The log dataset has files in JSON format generated from an event simulator based on the songs. They simulate app activity logs from an imaginary Sparkify streaming app. The log files are partitioned by year and month.

### Implementation
To optimize query performance for song play analysis, we want to create a star schema centered around song plays

Fact table
* songplays - records associated with song plays (aka. NextSong)

Dimension tables
* users
* songs
* artists
* time - timestamps of the records in songplays

We will distribute the `user`, `artist`, and `time` tables using the distribution style ALL so that there is a replica of each table on each slice.

`songplays` is the event based fact table so we will distribute it across slices using `song_id` as the distribution key. Likewise, `songs` will also be distributed by `song_id`.
