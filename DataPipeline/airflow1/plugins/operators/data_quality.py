from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 non_null_cols=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.non_null_cols = non_null_cols

    def execute(self, context):
        self.log.info("Starting Data Quality check for {}".format(self.table))

        redshift = PostgresHook(self.redshift_conn_id)
        self.validateHasRows(context, redshift)
        self.validateNonNullCols(context, redshift)

    def validateHasRows(self, context, redshift_hook):
        result = redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(self.table))
        if len(result) < 1 or len(result[0]) < 1:
            raise ValueError("Has Rows check failed. {} returned no results".format(self.table))
        record_count = result[0][0]
        if record_count < 1:
            raise ValueError("Has Rows check failed. {} contained 0 rows")
        self.log.info("Has rows check on {0} passed with {1} records".format(self.table, record_count))

    def validateNonNullCols(self, context, redshift_hook):
        for col in self.non_null_cols:
            result = redshift_hook.get_records("""
              SELECT COUNT(*)
              FROM {table}
              WHERE {col} IS NULL
            """.format(table=self.table, col=col))
            if len(result) < 1 or len(result[0]) < 1:
                raise ValueError("Non Null check failed. {} returned no results".format(self.table))
            record_count = result[0][0]
            if record_count > 0:
                raise ValueError(
                    "Non Null check failed. {table} had {records} rows with null {col}".format(table=self.table,
                                                                                               records=record_count,
                                                                                               col=col))
            self.log.info("Non Null check passed for {col} on {table}".format(col=col, table=self.table))
