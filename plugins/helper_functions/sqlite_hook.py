from airflow.hooks.base_hook import BaseHook
import sqlite3
import logging

class SQLiteHook(BaseHook):
    def __init__(self, sqlite_conn_id):
        self.sqlite_conn_id = sqlite_conn_id
        self.conn = None

    def get_conn(self):
        if self.conn is None:
            conn_info = self.get_connection(self.sqlite_conn_id)
            self.conn = sqlite3.connect(conn_info.host)
        return self.conn

    def close_conn(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
