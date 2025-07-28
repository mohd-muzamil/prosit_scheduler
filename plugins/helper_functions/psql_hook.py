from airflow.hooks.postgres_hook import PostgresHook
import psycopg2
from psycopg2 import OperationalError

class PsqlHook(PostgresHook):
    """
    Interact with PostgreSQL.
    """

    def __init__(self, psql_conn_id):
        """
        :param psql_conn_id: The Airflow connection ID for PostgreSQL.
        """
        super().__init__(postgres_conn_id=psql_conn_id)
        self.psql_conn_id = psql_conn_id
        self.conn = None

    def get_conn(self):
        """
        Establish a connection to PostgreSQL.
        """
        if self.conn is not None:
            return self.conn

        conn_info = self.get_connection(self.psql_conn_id)
        
        try:
            self.conn = psycopg2.connect(
                host=conn_info.host,
                port=conn_info.port,
                user=conn_info.login,
                password=conn_info.password,
                database=conn_info.schema
            )
        except OperationalError as e:
            self.log.error("Error during PostgreSQL connection: %s", str(e))
            raise

        return self.conn

    def close_conn(self):
        """
        Close the connection to PostgreSQL.
        """
        if self.conn:
            self.conn.close()
