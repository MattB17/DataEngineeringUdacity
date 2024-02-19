from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_table_statement="",
                 destination_table="",
                 table_select_statement="",
                 execute_drop=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_table_statement = create_table_statement
        self.destination_table = destination_table
        self.table_select_statement = table_select_statement
        self.execute_drop = execute_drop

    def execute(self, context):
        self.log.info('Starting insert for table {}'.format(self.destination_table))

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if (self.execute_drop):
            redshift.run("DROP TABLE IF EXISTS {}".format(self.destination_table))

        redshift.run(self.create_table_statement)

        insert_command = """
          INSERT INTO {dest_table} (
              {select_stmt}
          );
        """.format(dest_table=self.destination_table,
                   select_stmt=self.table_select_statement)

        redshift.run(insert_command)
        self.log.info("Completed insert for dimension table {}".format(self.destination_table))
