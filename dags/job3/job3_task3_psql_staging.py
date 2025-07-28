'''
Description: This script will copy the documents from mongodb to psql
Loki mongoDB entries -> Vidar psqlDB staging_db
'''
from job3 import *

# method to decrypt the attribute data.
def decrypt_value_column(enc_data, secret_key):
    try:
        enc_dict = json.loads(enc_data)
        iv = bytes.fromhex(enc_dict['iv'])
        content = bytes.fromhex(enc_dict['content'])

        cipher = Cipher(algorithms.AES(bytes.fromhex(secret_key)), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()

        decrypted_padded = decryptor.update(content) + decryptor.finalize()
        padding_length = decrypted_padded[-1]
        decrypted = decrypted_padded[:-padding_length]

        return decrypted.decode('utf-8')
    except Exception as e:
        logging.error(f"Error decrypting data: {e}")
        return enc_data


# method to decrypt the location data.
def decrypt_location_data(loc, k):
    # Assuming rn is your decryption module
    try:
        if isinstance(loc, list):
            loc = str(loc[0])
        else:
            loc = str(loc)
        if loc and isinstance(loc, (str, bytes)):
            return rn.decrypt(base64.b64decode(loc), k)
    except Exception as e:
        # print(f"Error decrypting location data: {e} \n Location data: {loc} \n Key: {k}")
        logging.error(f"Error decrypting location data: {e} \n Location data: {loc} \n Key: {k}")
        return None

def process_documents(documents, secret_key, encryption_key_location):
    df = pd.DataFrame(documents)
    # make sure df has the required columns
    # measuredAt, uploadedAt
    # if measuredat not in columns then add it with None values
    if 'measuredAt' not in df.columns:
        df['measuredAt'] = None
    if 'uploadedAt' not in df.columns:
        df['uploadedAt'] = None

    # Decrypt the 'value' column
    df['value'] = df['value'].apply(lambda x: decrypt_value_column(x, secret_key))

    # if df['value'] is a list, split it into separate columns as value0, value1, value2, ...
    # Check if any entry in 'value' is a list
    if df['value'].apply(lambda x: isinstance(x, list)).any():
        # Find the maximum length of the lists in the 'value' column
        max_len = df['value'].apply(lambda x: len(x) if isinstance(x, list) else 0).max()
        
        # Create new columns for each index in the list
        for i in range(max_len):
            df[f'value{i}'] = df['value'].apply(lambda x: x[i] if isinstance(x, list) and len(x) > i else None)
        
        # Drop the original 'value' column
        df = df.drop(columns=['value'])

    # Process each distinct attribute
    attributes_data = {}
    distinct_attributes = df['attribute'].unique()
    
    for attribute in distinct_attributes:
        df_attribute = df[df['attribute'] == attribute].copy()
        
        # Check for location attribute and decrypt
        if attribute.lower() == 'location':
            df_attribute['location_decrypted'] = df_attribute['value'].apply(decrypt_location_data, k=encryption_key_location)
        
        attributes_data[attribute.lower().replace(' ', '_')] = df_attribute
    
    return attributes_data


def insert_documents_to_psql(target_conn, db_name, schema_name, attributes_data, dryrun, time_range):
    for table_name, df_attribute in attributes_data.items():
        if dryrun:
            logging.info(f"DRYRUN: Would have copied {len(df_attribute)} documents to '{db_name}'.'{schema_name}'.'{table_name}' for datetime: {time_range}. Skipping the copy operation.")
        else:
            insert_or_update_psql(conn=target_conn, db_name=db_name, schema_name=schema_name, table_name=table_name, documents=df_attribute.to_dict(orient='records'))
            logging.info(f"{len(df_attribute)} documents copied to psql table '{db_name}'.'{schema_name}'.'{table_name}'")


def copy_documents_from_mongodb_to_psql(config: dict) -> None:
    source_conn_id = config["source_conn_id"]
    target_conn_id = config["target_conn_id"]
    target_db_name = config["target_db_name"]
    encryption_key_location = config["encryption_key_location"]
    secret_key = config["secret_key"]
    start_date_time = config["start_date_time"]
    end_date_time = config["end_date_time"]
    dryrun = config["dryrun"]

    source_mongo_hook = MongoHook(mongo_conn_id=source_conn_id)
    target_psql_hook = PsqlHook(psql_conn_id=target_conn_id)
    target_conn = target_psql_hook.get_conn()

    logging.info(f"Copying data from MongoDB to '{target_db_name}' for datetime: {start_date_time} to {end_date_time}")

    source_dbs = [db for db in source_mongo_hook.list_databases() if db.startswith('op1_study_')]

    if not source_dbs:
        logging.info("No databases found starting with 'op1_study_'.")
        return

    time_filter = {
        "uploadedAt": {
            "$gt": start_date_time,
            "$lte": end_date_time
        }
    }

    for source_db_name in source_dbs:
        source_db = source_mongo_hook.get_database(database=source_db_name)

        source_collections_names = [
            col for col in source_db.list_collection_names() 
            if source_db[col].count_documents(time_filter) > 0
        ]
        target_schema_name = f"{source_db_name}"
        
        for source_collection_name in source_collections_names:
            logging.info(f"MongoDB Query: {time_filter}")

            documents = list(source_db[source_collection_name].find(time_filter))
            document_count = len(documents)

            if document_count == 0:
                logging.info(f"No documents found for datetime: {start_date_time} to {end_date_time}.")
                continue
            else:
                logging.info(f"Found {document_count} documents for datetime: {start_date_time} to {end_date_time}.")

            attributes_data = process_documents(documents, secret_key, encryption_key_location)

            if not attributes_data:
                logging.info(f"No documents found after processing for datetime: {start_date_time} to {end_date_time}.")
                continue
            else:
                insert_documents_to_psql(target_conn, target_db_name, target_schema_name, attributes_data, dryrun, (start_date_time, end_date_time))

    logging.info("Documents copied successfully.")
    source_mongo_hook.close_conn()
    target_conn.close()
    