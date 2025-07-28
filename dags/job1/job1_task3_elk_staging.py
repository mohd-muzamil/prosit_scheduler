"""
Description: This script copies documents from MongoDB to Elasticsearch
Loki MongoDB entries -> Vidar Elasticsearch staging_db
"""
from job1 import *

def decrypt_location_data(loc, k):
    try:
        if isinstance(loc, list):
            loc = str(loc[0])
        else:
            loc = str(loc)
        if loc and isinstance(loc, (str, bytes)):
            return rn.decrypt(base64.b64decode(loc), k)
    except Exception as e:
        logging.error(f"Error decrypting location data: {e} \n Location data: {loc} \n Key: {k}")
        return None
    

def copy_documents_from_mongodb_to_elastic_search(config: dict) -> None:
    """
    Copy data from staging_db MongoDB in Loki to Elasticsearch in Vidar
    config: dictionary with source and target DB connection details
    """
    # Extract configuration parameters
    source_conn_id = config["source_conn_id"]
    target_conn_id = config["target_conn_id"]
    start_date_time = config["start_date_time"]
    end_date_time = config["end_date_time"]
    secret_key = config["encryption_key_location_new"]
    dryrun = config["dryrun"]
    batch_size = config.get("batch_size", 1000)

    # Initialize MongoDB and Elasticsearch connections
    source_mongo_hook = MongoHook(mongo_conn_id=source_conn_id)
    source_conn = source_mongo_hook.get_conn()
    # we'll be using the staging_db in the source MongoDB
    source_db_name = 'study_staging'
    source_collection_name = 'all'
    target_es_hook = ElasticsearchHook(es_conn_id=target_conn_id)
    target_conn = target_es_hook.get_conn()

    logging.info(f"Copying data from study_staging.all to Elasticsearch index in '{target_conn_id}' for datetime: {start_date_time} to {end_date_time}")
    
    # Define time filter for MongoDB query
    time_filter = {
        "uploadedAt": {
            "$gt": start_date_time,
            "$lte": end_date_time
        }
    }

    # MongoDB projection to preprocess data
    projection = {
        "_id": {"$toString": "$_id"},  # Convert ObjectId to string
        "attribute": 1,
        "measuredAt": {"$dateToString": {"format": "%Y-%m-%dT%H:%M:%S.%LZ", "date": "$measuredAt"}},
        "uploadedAt": {"$dateToString": {"format": "%Y-%m-%dT%H:%M:%S.%LZ", "date": "$uploadedAt"}},
        "study": 1,
        "participantId": 1,
        "value": 1  # Include the 'value' field for processing
    }

    def process_value_field(doc, key):
        """
        Process the 'value' field if it contains sensitive or location data.
        """
        if 'value' in doc:
            # check if doc['value'] is a list and take the first element
            if 'attribute' in doc and doc['attribute'].lower() == 'location':
                doc['value'] = decrypt_location_data(doc['value'], key)
                # split doc['value'] and keep only country which is the last value after last ',' 
                # 'sample data: 44.6393 | -63.5839 | None | None | None | None | University Ave, Halifax, NS, B3H 4H7, Canada'
                if doc['value'] is not None:
                    split_value = doc['value'].split(',')
                    doc['value'] = split_value[-1].strip() if len(split_value) > 1 else None
                else:
                    doc['value'] = None
            else:
                doc.pop('value')  # Remove the 'value' field if unnecessary
        return doc

    def generate_actions(cursor, index_name):
        """
        Generator function to prepare Elasticsearch bulk actions.
        """
        for doc in cursor:
            processed_doc = process_value_field(doc, secret_key)
            yield {
                "_op_type": "index",
                "_index": index_name,  # Use participantId as the index name
                "_id": doc["_id"],  # Metadata field
                "_source": {key: value for key, value in processed_doc.items() if key != '_id'}  # Exclude _id from _source
            }

    # Fetch distinct studies and participants using aggregation
    logging.info("Fetching distinct studies and participants...")
    aggregation = [
        {"$match": time_filter},
        {"$group": {"_id": {"study": "$study", "participantId": "$participantId"}}}
    ]

    source_db = source_conn[source_db_name]
    grouped_results = source_db[source_collection_name].aggregate(aggregation)
    grouped_data = list(grouped_results)

    if not grouped_data:
        logging.info(f"No documents found for datetime: {start_date_time} to {end_date_time} to copy into Elasticsearch.")
        return

    # Iterate over studies and participants
    for group in grouped_data:
        study = group["_id"]["study"]
        participant_id = group["_id"]["participantId"]
        
        if not study or not participant_id:
            logging.warning(f"Skipping group with missing study or participantId: {group['_id']}")
            continue
        target_index_name = f"study_{study}" #example: study_prositaia
        # target_index_name = "gaya" if "gaya" in participant_id else participant_id    #example: gaya or prositaia123
        # target_index_name = source_db_name + "_gaya" if "gaya" in source_collection_name else source_db_name + "_" + source_collection_name   #example: study_staging_gaya or study_staging_all

        # Query documents for the specific participant
        participant_filter = {"participantId": participant_id}
        combined_filter = {**time_filter, **participant_filter}
        cursor = source_db[source_collection_name].find(combined_filter, projection).batch_size(batch_size)

        if dryrun:
            logging.info(f"DRYRUN: Would have copied documents for participant '{participant_id}' in study '{study}'. Skipping actual copy.")
        else:
            # Perform bulk insert into Elasticsearch
            success, failed = bulk(
                client=target_conn,
                actions=generate_actions(cursor, target_index_name),
                chunk_size=batch_size,
                raise_on_error=False
            )
            logging.info(f"Successfully inserted {success} documents into Elasticsearch index '{target_index_name}'.")

            if failed:
                logging.error(f"Failed to insert {len(failed)} documents into Elasticsearch index '{target_index_name}'. Sample error: {failed[0].get('index', {}).get('error', {})}")

    # Refresh Elasticsearch indices after bulk operations
    if not dryrun:
        target_conn.indices.refresh()

    logging.info(f"Documents copied from staging_db MongoDB to Vidar Elasticsearch.")
    source_mongo_hook.close_conn()
    target_es_hook.close_conn()
