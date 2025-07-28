from airflow.models import Connection

def get_mongo_conn_str(conn_id):
    '''
    Get connection from Airflow secrets
    '''
    conn = Connection.get_connection_from_secrets(conn_id)
    conn_str = f"mongodb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return conn_str