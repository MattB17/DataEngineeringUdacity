from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
      COPY {dest_table}
      FROM '{s3_path}'
      ACCESS_KEY_ID '{s3_access_key}'
      SECRET_ACCESS_KEY '{s3_secret_key}'
      FORMAT as json '{json_format}'
      REGION '{region}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 create_table_statement="",
                 destination_table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.create_statement = create_table_statement
        self.destination_table = destination_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_format = json_format

    def execute(self, context):
        self.log.info('Starting task to load s3 data to {}'.format(self.destination_table))

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info("Recreating {} pre-upload".format(self.destination_table))
        redshift.run("DROP TABLE IF EXISTS {}".format(self.destination_table))
        redshift.run(self.create_statement)

        self.log.info("Copying from S3 to {}".format(self.destination_table))
        s3_resource_key = self.s3_key.format(**context)
        s3_path = "s3://{bucket}/{key}".format(bucket=self.s3_bucket, key=s3_resource_key)
        copy_command = StageToRedshiftOperator.copy_sql.format(
          dest_table=self.destination_table,
          s3_path=s3_path,
          s3_access_key=credentials.access_key,
          s3_secret_key=credentials.secret_key,
          json_format=self.json_format,
          region=self.region
        )

        redshift.run(copy_command)
        self.log.info("Upload to {} completed".format(self.destination_table))
