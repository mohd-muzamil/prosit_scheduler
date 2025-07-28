'''
Description: copy the documents from OP1 mongodb to Loki mongodb, segregating the documents into different studies based on partcipantId.
Loki mongoDB entries -> Loki mongoDB studies
'''
from job3 import *


def fix_participantId(participantId: str) -> str:
    '''
    Just return the participantId as is.
    '''
    # prefix_match = re.findall(r"([A-Za-z]+)\d*", participantId)
    # suffix_match = re.findall(r"[A-Za-z]*(\d+.*)", participantId)
    # prefix = prefix_match[0].strip() if prefix_match else ""
    # suffix = suffix_match[0].lstrip("0").strip() if suffix_match else ""
    # return f"{prefix}_{suffix}"
    # return f"{prefix}{suffix}".lower()
    return participantId.lower()

def copy_documents_from_mongodb_to_mongodb(config: dict) -> int:
    '''
    Segregate the documents into different studies based on participantId.
    config: dictionary with MongoDB connection details
    '''
    source_conn_id = config["source_conn_id"]
    target_conn_id = config["target_conn_id"]
    start_date_time = config["start_date_time"]
    end_date_time = config["end_date_time"]
    dryrun = config["dryrun"]
    
    source_mongo_hook = MongoHook(mongo_conn_id=source_conn_id)
    target_mongo_hook = MongoHook(mongo_conn_id=target_conn_id)

    source_db_names = source_mongo_hook.list_databases()
    source_db_names = [db_name for db_name in source_db_names if db_name.startswith('study_')]

    if not source_db_names:
        logging.info("No databases found starting with 'study_'.")
        return 0
    
    time_filter = {
        "uploadedAt": {
            "$gte": start_date_time,
            "$lt": end_date_time
        }
    }

    # Iterate over source databases that start with 'study_'
    for source_db_name in source_db_names:
        source_db = source_mongo_hook.get_database(database=source_db_name)

        source_collections_names = [
            col for col in source_db.list_collection_names() 
            if source_db[col].count_documents(time_filter) > 0
        ]

        if not source_collections_names:
            logging.info(f"No collections found with data in the given time range for database: {source_db_name}.")
            continue
        
        for source_collection_name in source_collections_names:
            logging.info(f"MongoDB Query: {time_filter}")

            documents = list(source_db[source_collection_name].find(time_filter))
            logging.info(f"Number of documents retrieved: {len(documents)}")

            if not documents:
                logging.info(f"No documents found for datetime: {start_date_time} to {end_date_time}.")
                continue
            
            df = pd.DataFrame(documents)

            if 'participantId' not in df.columns:
                logging.error("No participantId column found in the dataframe.")
                continue
            
            # Preprocessing
            df['participantId'] = df['participantId'].apply(lambda participantId: fix_participantId(participantId))

            if len(df.participantId.unique()) == 1:
                logging.info(f"One unique participantId found: {df.participantId.unique()}")
            else:
                logging.error(f"Multiple unique participantIsd found for a single collection in the database: {source_db_name}.{source_collection_name}: {df.participantId.unique()}")

            target_collection_name = df.participantId.unique()[0]
            target_db_name = f"op1_{source_db_name}"
            target_db = target_mongo_hook.get_database(database=target_db_name)

            if dryrun:
                logging.info(f"DRYRUN: Would have inserted {len(df)} documents into {target_db_name}.{target_collection_name}")
            else:
                insert_or_update_mongodb(target_db, target_collection_name, df.to_dict('records'))

            logging.info(f"Segregated the documents into different studies based on participantId for datetime: {start_date_time} to {end_date_time}")

    source_mongo_hook.close_conn()
    target_mongo_hook.close_conn()

    return 0
