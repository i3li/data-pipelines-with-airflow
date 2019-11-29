from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """This operator enables data quality checks for a Postgres database."""

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='postgres',
                 sql_stmts=(),
                 result_checkers=(),
                 *args, **kwargs):
        """
        :param postgres_conn_id: The postgres connection id. The name or identifier for
        establishing a connection to the Postgrea database.
        :type postgres_conn_id: str
        :param sql_stmts: A collection of sql statments the are used for data quality checks. This should be a tuple
        that contains strings.
        :type sql_stmts: tuple
        :param result_checkers: A collection of checkers the are used to test the result of the executed sql statements.
        A checker is a function that takes the retrieved records and return a bool. This should be a tuple containing functions.
        Each passed checker corresponds to the sql statement that is placed in the same index in `sql_stmts`.
        :type result_checkers: tuple
        """
        if len(sql_stmts) != len(result_checkers):
            raise Exception('sql_stmts and result_checkers must have the same number of elements.')
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_stmts = sql_stmts
        self.result_checkers = result_checkers

    def execute(self, context):
        # Postgres Hook
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        for sql_stmt, result_checker in zip(self.sql_stmts, self.result_checkers):
            records = postgres.get_records(sql_stmt)
            if not result_checker(records):
                raise ValueError('Data quality check failed. SQL: {}'.format(sql_stmt))
            self.log.info("Passed Quality Check: '{}'.".format(sql_stmt))
