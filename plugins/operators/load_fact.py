from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """This operator enables loading fact tables."""

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 table='',
                 select_sql_stmt='',
                 *args, **kwargs):
        """
        :param redshift_conn_id: The redshift connection id. The name or identifier for
        establishing a connection to Redshift.
        :type redshift_conn_id: str
        :param aws_credentials_id: The AWS connection id. The name or identifier fo
        establishing a connection to AWS
        :type aws_credentials_id: str
        :param table: The name of the table where the data should be loadede.
        :type table: str
        :param select_sql_stmt: The SQL statment that is used to retrieve the fact table data.
        :type select_sql_stmt: str
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.select_sql_stmt = select_sql_stmt

    def execute(self, context):
        # AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        # Postgres Hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql_stmt = 'INSERT INTO {} ({})'.format(self.table, self.select_sql_stmt)

        # Load table
        self.log.info("Loading data into {}.".format(self.table))
        redshift.run(insert_sql_stmt)
