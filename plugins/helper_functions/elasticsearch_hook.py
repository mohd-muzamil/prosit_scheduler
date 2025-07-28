from airflow.hooks.base_hook import BaseHook
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError, NotFoundError, ConnectionError  # Add other exceptions as needed

class ElasticsearchHook(BaseHook):
    """
    Interact with Elasticsearch.
    """

    def __init__(self, es_conn_id):
        """
        :param es_conn_id: The Airflow connection ID for Elasticsearch.
        """
        super().__init__()
        self.es_conn_id = es_conn_id
        self.conn = None

    def get_conn(self):
        """
        Establish a connection to Elasticsearch.
        """
        if not self.conn:
            conn_info = self.get_connection(self.es_conn_id)
            hosts = [{
                'host': conn_info.host,
                'port': conn_info.port,
                'scheme': conn_info.schema if conn_info.schema else 'http'
            }]
            self.conn = Elasticsearch(
                hosts=hosts,
                http_auth=(conn_info.login, conn_info.password) if conn_info.login else None
            )
        return self.conn

    def close_conn(self):
        """
        Close the connection to Elasticsearch.
        """
        if self.conn:
            self.conn.transport.close()
            self.conn = None

    def get_index(self, index_name):
        """
        Get an Elasticsearch index.
        :param index_name: Name of the Elasticsearch index.
        :return: Index information if exists, otherwise None.
        """
        es = self.get_conn()
        try:
            if es.indices.exists(index=index_name):
                return es.indices.get(index=index_name)
            else:
                return None
        except (TransportError, NotFoundError, ConnectionError) as e:
            self.log.error(f"Error getting index {index_name}: {str(e)}")
            return None

    def create_index(self, index_name, body=None):
        """
        Create an Elasticsearch index.
        :param index_name: Name of the Elasticsearch index.
        :param body: Index configuration (mappings/settings).
        :return: Index creation response.
        """
        es = self.get_conn()
        try:
            if not es.indices.exists(index=index_name):
                return es.indices.create(index=index_name, body=body)
            else:
                self.log.info(f"Index {index_name} already exists.")
                return None
        except (TransportError, NotFoundError, ConnectionError) as e:
            self.log.error(f"Error creating index {index_name}: {str(e)}")
            return None

    def get_document(self, index_name, doc_id):
        """
        Get a document from an Elasticsearch index.
        :param index_name: Name of the Elasticsearch index.
        :param doc_id: Document ID.
        :return: Document information if exists, otherwise None.
        """
        es = self.get_conn()
        try:
            return es.get(index=index_name, id=doc_id)
        except (TransportError, NotFoundError, ConnectionError) as e:
            self.log.error(f"Error getting document {doc_id} from index {index_name}: {str(e)}")
            return None
