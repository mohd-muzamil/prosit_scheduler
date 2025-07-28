'''
Description: Copy data from one MongoDB collection to another
Frigg2 mongoDB entries -> Loki mongoDB entries
'''
from job1 import *

def copy_documents_from_mongodb_to_mongodb(config: dict) -> None:
    '''
    Copy data from one MongoDB collection to another
    config: dictionary with source and target MongoDB connection details
    args:
        config: dictionary with source and target MongoDB connection details
    returns:
        None
    '''
    source_conn_id = config["source_conn_id"]
    target_conn_id = config["target_conn_id"]
    source_db_name = config["source_db_name"]
    target_db_name = config["target_db_name"]
    source_collection_name = config["source_collection_name"]
    target_collection_name = config["target_collection_name"]
    start_date_time = config["start_date_time"]
    end_date_time = config["end_date_time"]
    dryrun = config["dryrun"]
    
    source_mongo_hook = MongoHook(mongo_conn_id=source_conn_id)
    source_db = source_mongo_hook.get_database(database=source_db_name)
    target_mongo_hook = MongoHook(mongo_conn_id=target_conn_id)
    target_db = target_mongo_hook.get_database(database=target_db_name)

    logging.info(f"Copying data from '{source_db_name}'.'{source_collection_name}' to '{target_db_name}'.'{target_collection_name}' for datetime: {start_date_time} to {end_date_time}  ")  

    # Create a PyMongo query (your actual query here)
    time_filter = {
        "uploadedAt": {
            "$gt": start_date_time,
            "$lte": end_date_time
        }
    }

    # logging.info the query used for the data copy
    logging.info(f"MongoDB Query: {time_filter}")

    documents_to_copy = list(source_db[source_collection_name].find(time_filter))
    if dryrun:
        logging.info(f"DRYRUN: Would have copied the {len(documents_to_copy)} documents from '{source_db_name}'.'{source_collection_name}' to '{target_db_name}'.'{target_collection_name}' for datetime: {start_date_time} to {end_date_time}. skipping the copy operation.")
    else:
        insert_or_update_mongodb(target_db, target_collection_name, documents_to_copy)
        logging.info(f"Copied {len(documents_to_copy)} documents from '{source_db_name}'.'{source_collection_name}' to '{target_db_name}'.'{target_collection_name}' for datetime: {start_date_time} to {end_date_time}.")

    source_mongo_hook.close_conn()
    target_mongo_hook.close_conn()

    return None