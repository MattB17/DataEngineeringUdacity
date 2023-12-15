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

### Pipeline
The entire pipeline can be run via the `SparkifyPipeline.ipynb` notebook.

This notebook performs the following
* loads the SQL extension
* sets up the IAM role and Redshift cluster via `cluster_setup.py`
* creates the staging and star schema tables via `create_tables.py`
* performs the ETL process of loading data into staging tables from S3 and then inserting that data into the star schema tables via `etl.py`
* executes some generic analysis queries to validate the data was loaded correctly
* deletes the cluster and IAM role via `cluster_teardown.py`

The queries for dropping tables, creating tables, loading data into the staging tables, and inserting data into the star schema can be found in `sql_queries.py`.
