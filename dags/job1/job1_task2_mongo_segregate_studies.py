'''
Description: Segregate the documents into different studies based on partcipantId.
Loki mongoDB entries -> Loki mongoDB studies
'''
from job1 import *


def fix_participantId(participantId: str) -> str:
    prefix_match = re.findall(r"([A-Za-z]+)\d*", participantId)
    suffix_match = re.findall(r"[A-Za-z]*(\d+.*)", participantId)

    prefix = prefix_match[0].strip() if prefix_match else ""
    suffix = suffix_match[0].lstrip("0").strip() if suffix_match else ""
    # return f"{prefix}_{suffix}"
    return f"{prefix}{suffix}".lower()
    

def decrypt_location(location: str, encryption_key: str) -> str:
    '''
    Decrypt the location data.
    location: location data to be decrypted
    encryption_key: encryption key for location
    args:
        location: location data to be decrypted
        encryption_key: encryption key for location
    returns:
        decrypted location data
    '''
    if isinstance(location, list):
        location = str(location[0])
    else:
        location = str(location)
    if location and isinstance(location, (str, bytes)):
        return rn.decrypt(base64.b64decode(location), encryption_key)


def encrypt_location(location: Union[list, str], encryption_key: str) -> str:
    '''
    Encrypt the location data.
    location: location data to be encrypted
    encryption_key: encryption key for location
    args:
        location: location data to be encrypted
        encryption_key: encryption key for location
    returns:
        encrypted location data
    '''
    if isinstance(location, list):
        location = str(location[0])
    else:
        location = str(location)
    if location and isinstance(location, (str, bytes)):
        return base64.b64encode(rn.encrypt(location, encryption_key)).decode('utf-8')


def segregate_documents_into_study_collections(config: dict) -> int:
    '''
    segregate the documents into different studies based on partcipantId.
    config: dictionary with MongoDB connection details
    '''
    conn_id = config["conn_id"]
    db_name = config["db_name"]
    collection_name = config["collection_name"]
    # lookup_db_conn_id = config["lookup_db_conn_id"]
    # lookup_table_name_location = config["lookup_table_name_location"]
    # lookup_table_name_device_type = config["lookup_table_name_device_type"]
    encryption_key_location = config["encryption_key_location"]
    encryption_key_location_new = config["encryption_key_location_new"]
    start_date_time = config["start_date_time"],
    end_date_time = config["end_date_time"],
    dryrun = config["dryrun"]
    
    mongo_hook = MongoHook(mongo_conn_id=conn_id)
    db = mongo_hook.get_database(database=db_name)

    time_filter = {
        "uploadedAt": {
            "$gt": start_date_time[0],
            "$lte": end_date_time[0]
        }
    }

    logging.info(f"Source DB: {db_name}, Collection: {collection_name}, Time filter: {time_filter}")
    documents = list(db[collection_name].find(time_filter))

    if len(documents) == 0:
        logging.info(f"No documents found for datetime: {start_date_time} to {end_date_time} to segregate into different studies based on partcipantId.")
        return 0
    else:
        df = pd.DataFrame(documents)

        if 'participantId' not in df.columns:
            logging.error("No participantId column found in the dataframe while processing for segregating the documents into different studies based on partcipantId.")
            return 0
        
        # preprocessing
        # fix participantId
        df['participantId'] = df['participantId'].apply(lambda participantId: fix_participantId(participantId))
        # segregate the documents into different studies based on first split of participantId on '_'
        df['study'] = df['participantId'].str.extract(r'(\D+)', expand=False).str.strip()

        # chage the location encryption key to new encryption key
        df['value'] = df.apply(lambda x: decrypt_location(x['value'], encryption_key_location) if x['attribute'].lower() == 'location' else x['value'], axis=1)
        df['value'] = df.apply(lambda x: encrypt_location(x['value'], encryption_key_location_new) if x['attribute'].lower() == 'location' else x['value'], axis=1)
        
        
        # Commenting below block as we will not staging frigg2 data into study_staging db, only op1 data will be staged
        # insert the documents into staging db called study_staging which has data from all the studies
        staging_db_name = "study_staging"
        staging_db = mongo_hook.get_database(database=staging_db_name)
        staging_collection_name = "all"
        
        if dryrun:
            logging.info(f"DRYRUN: Would have segregated{len(df)} documents into different studies based on partcipantId for datetime: {start_date_time} to {end_date_time}  ")
        else:
            insert_or_update_mongodb(staging_db, staging_collection_name, df.to_dict('records'))
            logging.info(f"Inserted {len(df)} documents into {staging_db_name}.{staging_collection_name} collection.")
        

        # segregating data into different studies based on the study column
        studies = list(df.study.unique())
        for study in studies:
            study_df = df[df.study == study]
            study_df = study_df.drop(['study'], axis=1)
            study_db_name = f"study_{study}"

            distinct_participantIds = study_df.participantId.unique()
            for participantId in distinct_participantIds:
                participantId_df = study_df[study_df.participantId == participantId]
                study_collection_name = f"{participantId}"
                participant_db = mongo_hook.get_database(database=study_db_name)
                
                if dryrun:
                    logging.info(f"DRYRUN: Would have inserted {len(participantId_df)} documents into study collection {study_db_name}.{study_collection_name} for datetime: {start_date_time} to {end_date_time}  ")
                else:
                    insert_or_update_mongodb(participant_db, study_collection_name, participantId_df.to_dict('records'))
                    # logging.info(f"Inserted {len(participantId_df)} documents into {study_db_name}.{study_collection_name} collection.")

        logging.info(f"Segregated the documents into different studies based on partcipantId for datetime: {start_date_time} to {end_date_time}  ")
    mongo_hook.close_conn()
    return None