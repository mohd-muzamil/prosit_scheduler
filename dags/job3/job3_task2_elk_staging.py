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

        if padding_length > 16:  # Validate PKCS7 padding
            logging.error(f"Invalid padding length: {padding_length}")
            return None

        decrypted = decrypted_padded[:-padding_length]
        return decrypted.decode('utf-8')
    except Exception as e:
        logging.error(f"Error decrypting data: {e}")
        return None

# method to decrypt the location data.
def decrypt_location_data(loc, k):
    try:
        if not loc:
            return None
        loc_str = str(loc[0]) if isinstance(loc, list) else str(loc)
        return rn.decrypt(base64.b64decode(loc_str), k)
    except Exception as e:
        # print(f"Error decrypting location data: {e} \n Location data: {loc} \n Key: {k}")
        logging.error(f"Error decrypting location data: {e} \n Location data: {loc} \n Key: {k}")
        return None

def process_location(location: str, secret_key: str, encryption_key_location: str) -> str:
    """Process 'location' field in the document and extract the country."""
    # Decrypt the location field using the secret key
    try:
        intermediate_location_decrypted = decrypt_value_column(location, secret_key)
        location_decrypted = decrypt_location_data(intermediate_location_decrypted, encryption_key_location)
    except Exception as e:
        logging.error(f"Error decrypting location field: {e}")
        return None
    
    # Extract the country by splitting the decrypted location
    if location_decrypted:
        try:
            # Split the location_decrypted string by commas and strip whitespace which represents country name
            return location_decrypted.split(',')[-1].strip()
        except Exception as e:
            logging.error(f"Error processing location field: {e}")
    return None


def copy_documents_from_mongodb_to_elastic_search(config: dict) -> int:
    source_conn_id = config["source_conn_id"]
    target_conn_id = config["target_conn_id"]
    encryption_key_location = config["encryption_key_location"]
    secret_key = config["secret_key"]
    start_date_time = config["start_date_time"]
    end_date_time = config["end_date_time"]
    dryrun = config["dryrun"]
    batch_size = config.get("batch_size", 1000)

    def serialize_document(doc):
        """Convert MongoDB document to a JSON-serializable format."""
        try:
            if isinstance(doc['_id'], ObjectId):
                doc['_id'] = str(doc['_id'])  # Convert MongoDB ObjectId to string
            if isinstance(doc.get('measuredAt'), datetime):
                doc['measuredAt'] = doc['measuredAt'].isoformat()  # Convert datetime to ISO format
            if isinstance(doc.get('uploadedAt'), datetime):
                doc['uploadedAt'] = doc['uploadedAt'].isoformat()  # Convert datetime to ISO format
            # if isinstance(doc.get('value'), dict):
                # Ensure 'value' is a JSON-serializable dictionary
                # doc['value'] = doc['value']  # Assuming 'value' is already in JSON-serializable format
            # if key called 'value' exists in the document, then drop it from the document
            decrypt_arrtibutes = ['device', 'systeminfo', 'analytics', 'reachability', 'connectivity']
            if 'value' in doc:
                # Decrypt the 'value' field using the secret key if value is location
                if 'location' in doc.get('attribute', '').lower():
                    doc['value'] = process_location(doc['value'], secret_key, encryption_key_location)
                # list of attributes to be decrypted using the secret key
                elif any(attr in doc.get('attribute', '').lower() for attr in decrypt_arrtibutes):
                    doc['value'] = decrypt_value_column(doc['value'], secret_key).lower()
                else:   
                    doc.pop('value', None)  # Safe removal without KeyError
            return doc
        except Exception as e:
            logging.error(f"Error serializing document {doc.get('_id', 'UNKNOWN')}: {e}")
            return None


    def generate_actions(source_db, collection_name, index_name):
        total_docs = source_db[collection_name].count_documents(time_filter)
        logging.info(f"Total documents to insert into Elasticsearch index '{index_name}': {total_docs}")
        cursor = source_db[collection_name].find(time_filter).batch_size(batch_size)
        
        for doc in cursor:
            serialized_doc = serialize_document(doc)  # Serialize document
            yield {
                "_op_type": "index",
                "_index": index_name,
                "_id": serialized_doc['_id'],  # Metadata field
                "_source": {key: value for key, value in serialized_doc.items() if key != '_id'}  # Exclude _id from _source
            }

    source_mongo_hook = MongoHook(mongo_conn_id=source_conn_id)
    target_es_hook = ElasticsearchHook(es_conn_id=target_conn_id)
    target_conn = target_es_hook.get_conn()

    source_dbs = source_mongo_hook.list_databases()
    source_dbs = [db for db in source_dbs if db.startswith('op1_study_')]

    if not source_dbs:
        logging.info("No databases found starting with 'study_'.")
        return 0
    
    time_filter = {
        "uploadedAt": {
            "$gt": start_date_time,
            "$lte": end_date_time
        }
    }

    for source_db_name in source_dbs:
        try:
            source_db = source_mongo_hook.get_database(database=source_db_name)
        except Exception as e:
            logging.error(f"Error connecting to MongoDB database '{source_db_name}': {e}")
            continue
        
        source_collections_names = [
            col for col in source_db.list_collection_names()
            if source_db[col].count_documents(time_filter) > 0
        ]

        if not source_collections_names:
            logging.info(f"No collections found with data in the given time range for database: {source_db_name}.")
            continue
        
        for source_collection_name in source_collections_names:
            logging.info(f"MongoDB Query: {time_filter}")

            if dryrun:
                try:
                    document_count = source_db[source_collection_name].count_documents(time_filter)
                    logging.info(f"DRYRUN: Would have inserted {document_count} documents into Elasticsearch index '{source_collection_name}'.")
                except Exception as e:
                    logging.error(f"Error during dry run for collection '{source_collection_name}': {e}")
                continue
            
            # study is source_collection_name split with number and first part is the source_db_name
            # Example: if source_collection_name = "prositms1000" then study = "prositms"
            target_index_name = source_db_name
            # target_index_name = f"{source_db_name}_{source_collection_name}" if "gaya" not in source_collection_name else target_index_name   #example: op1_study_gaya_prositms1000 retired this index_naming
            
            # Check if the target index exists before inserting
            if not target_conn.indices.exists(index=target_index_name):
                target_conn.indices.create(index=target_index_name)
            
            # Perform bulk insert
            try:
                success, failed = bulk(
                    client=target_conn,
                    actions=generate_actions(source_db, source_collection_name, target_index_name),
                    chunk_size=batch_size,
                    raise_on_error=False
                )
                logging.info(f"Inserted {success} documents into Elasticsearch index '{source_collection_name}'.")
                
                if failed:
                    logging.error(f"Failed to insert {len(failed)} documents into Elasticsearch index '{source_collection_name}'.")
                    for item in failed:
                        if 'index' in item and item['index'].get('error'):
                            logging.error(f"Failed document: {item['index']['error']}")
            except Exception as e:
                logging.error(f"Error during bulk insert into Elasticsearch index '{source_collection_name}': {e}")
            
            # Check if the index exists before refreshing
            if target_conn.indices.exists(index=source_collection_name):
                target_conn.indices.refresh(index=source_collection_name)
            else:
                logging.warning(f"Skipping refresh: Index '{source_collection_name}' does not exist.")

        try:
            source_db.client.close()
        except Exception as e:
            logging.error(f"Error closing MongoDB connection for database '{source_db_name}': {e}")

    source_mongo_hook.close_conn()
    target_es_hook.close_conn()

    return 0
