from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 table='',
                 select_sql_stmt='',
                 truncate_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.select_sql_stmt = select_sql_stmt
        self.truncate_insert = truncate_insert

    def execute(self, context):
        # AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # Postgres Hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        insert_sql_stmt = 'INSERT INTO {} ({})'.format(self.table, self.select_sql_stmt)
        
        if self.truncate_insert:
            truncate_sql_stmt='DELETE FROM {}'.format(self.table)
            # Truncate table
            redshift.run(truncate_sql_stmt)
        
        # Load table
        redshift.run(insert_sql_stmt)

