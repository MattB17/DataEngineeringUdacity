from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

S3_BUCKET = "mattbx17-udacity-airflow"
REDSHIFT_CONN_ID = "redshift"
AWS_CREDENTIALS_ID = "aws_credentials"
AWS_REGION = "us-east-1"

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        create_table_statement=final_project_sql_statements.SqlQueries.staging_events_table_create,
        destination_table="staging_events",
        s3_bucket=S3_BUCKET,
        s3_key="log-data/",
        json_format="s3://{}/log_json_path.json".format(S3_BUCKET),
        region=AWS_REGION
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        create_table_statement=final_project_sql_statements.SqlQueries.staging_songs_table_create,
        destination_table="staging_songs",
        s3_bucket=S3_BUCKET,
        s3_key="song-data/",
        json_format="auto",
        region=AWS_REGION
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        create_table_statement=final_project_sql_statements.SqlQueries.songplay_table_create,
        destination_table="public.songplays",
        table_select_statement=final_project_sql_statements.SqlQueries.songplay_table_insert,
        execute_drop=True
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        create_table_statement=final_project_sql_statements.SqlQueries.user_table_create,
        destination_table="public.user",
        table_select_statement=final_project_sql_statements.SqlQueries.user_table_insert,
        execute_drop=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        create_table_statement=final_project_sql_statements.SqlQueries.song_table_create,
        destination_table="public.song",
        table_select_statement=final_project_sql_statements.SqlQueries.song_table_insert,
        execute_drop=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        create_table_statement=final_project_sql_statements.SqlQueries.artist_table_create,
        destination_table="public.artist",
        table_select_statement=final_project_sql_statements.SqlQueries.artist_table_insert,
        execute_drop=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        create_table_statement=final_project_sql_statements.SqlQueries.time_table_create,
        destination_table="public.time",
        table_select_statement=final_project_sql_statements.SqlQueries.time_table_insert,
        execute_drop=True
    )

    songplays_quality_check = DataQualityOperator(
        task_id='songplays_quality_check',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="public.songplays",
        non_null_cols=["songplay_id", "start_time", "user_id", "song_id", "artist_id", "session_id"]
    )

    user_quality_check = DataQualityOperator(
        task_id='user_quality_check',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="public.user",
        non_null_cols=["user_id"]
    )

    song_quality_check = DataQualityOperator(
        task_id='song_quality_check',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="public.song",
        non_null_cols=["song_id", "artist_id", "year", "duration"]
    )

    artist_quality_check = DataQualityOperator(
        task_id='artist_quality_check',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="public.artist",
        non_null_cols=["artist_id"]
    )

    time_quality_check = DataQualityOperator(
        task_id='time_quality_check',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table="public.time",
        non_null_cols=["start_time"]
    )

    end_operator = DummyOperator(task_id="End_execution")

    # First task is to upload data to staging tables, which can
    # be done in parallel.
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    # Next we build the fact table from the data in staging, so those
    # tasks must be finished first.
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    # We can now build the dimension tables which depend on the fact table.
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    # We can also start quality checks on the songplays tables
    load_songplays_table >> songplays_quality_check

    # Once all the tables can be loaded we can perform data quality checks.
    load_user_dimension_table >> user_quality_check
    load_song_dimension_table >> song_quality_check
    load_artist_dimension_table >> artist_quality_check
    load_time_dimension_table >> time_quality_check

    # After quality checks complete, we are finished.
    songplays_quality_check >> end_operator
    user_quality_check >> end_operator
    song_quality_check >> end_operator
    artist_quality_check >> end_operator
    time_quality_check >> end_operator

final_project_dag = final_project()
