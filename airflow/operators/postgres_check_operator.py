from airflow.hooks import PostgresHook
from airflow.operators import CheckOperator, ValueCheckOperator, IntervalCheckOperator
from airflow.utils import apply_defaults


class PostgresCheckOperator(CheckOperator):
    """
    Performs checks against Postgres. The ``PostgresCheckOperator`` expects
    a sql query that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.

    Note that Python bool casting evals the following as ``False``:
    * False
    * 0
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alterts
    without stopping the progress of the DAG.

    :param sql: the sql to be executed
    :type sql: string
    :param postgres_conn_id: reference to the Postgres database
    :type postgres_conn_id: string
    """

    @apply_defaults
    def __init__(
            self, sql,
            postgres_conn_id='postgres_default',
            *args, **kwargs):
        super(PostgresCheckOperator, self).__init__(sql=sql, *args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.sql = sql

    def get_db_hook(self):
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)


class PostgresValueCheckOperator(ValueCheckOperator):
    """
    Performs a simple value check using sql code.

    :param sql: the sql to be executed
    :type sql: string
    :param postgres_conn_id: reference to the Postgres database
    :type postgres_conn_id: string
    """

    @apply_defaults
    def __init__(
            self, sql, pass_value, tolerance=None,
            postgres_conn_id='postgres_default',
            *args, **kwargs):
        super(PostgresValueCheckOperator, self).__init__(sql, pass_value, tolerance, *args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def get_db_hook(self):
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)


class PostgresIntervalCheckOperator(IntervalCheckOperator):
    """
    Checks that the values of metrics given as SQL expressions are within
    a certain tolerance of the ones from days_back before.

    :param table: the table name
    :type table: str
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :type days_back: int
    :param metrics_threshold: a dictionary of ratios indexed by metrics
    :type metrics_threshold: dict
    :param postgres_conn_id: reference to the Postgres database
    :type postgres_conn_id: string
    """

    @apply_defaults
    def __init__(
            self, table, metrics_thresholds,
            date_filter_column='ds', days_back=-7,
            postgres_conn_id='postgres_default',
            *args, **kwargs):
        super(PostgresIntervalCheckOperator, self).__init__(table, metrics_thresholds, date_filter_column, days_back, *args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def get_db_hook(self):
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)
