# MongoDB hook for Airflow
from airflow.hooks.base_hook import BaseHook
from pymongo import MongoClient

class MongoHook(BaseHook):
    """
    Interact with MongoDB.
    """

    def __init__(self, mongo_conn_id):
        """
        :param mongo_conn_id: The Airflow connection ID for MongoDB.
        """
        self.mongo_conn_id = mongo_conn_id
        self.conn = None

    def get_conn(self):
        """
        Establish a connection to MongoDB.
        """
        conn_info = self.get_connection(self.mongo_conn_id)
        conn_uri = f"mongodb://{conn_info.login}:{conn_info.password}@{conn_info.host}:{conn_info.port}/{conn_info.schema}"
        self.conn = MongoClient(conn_uri)
        return self.conn

    def close_conn(self):
        """
        Close the connection to MongoDB.
        """
        if self.conn:
            self.conn.close()

    def list_databases(self):
        """
        List all databases in MongoDB.
        """
        conn = self.get_conn()
        return conn.list_database_names()

    def get_database(self, database):
        """
        Get a MongoDB database.
        """
        conn = self.get_conn()
        return conn[database]
    
    def get_collection(self, database, collection):
        """
        Get a MongoDB collection.
        """
        db = self.get_database(database)
        return db[collection]


# Example usage in an Airflow DAG
# mongo_hook = MongoHook(mongo_conn_id='your_custom_mongo_conn_id')
# connection = mongo_hook.get_conn()
# my_database = mongo_hook.get_database(database='your_database')
# my_collection = mongo_hook.get_collection(database='your_database', collection='your_collection')
# mongo_hook.close_conn()
