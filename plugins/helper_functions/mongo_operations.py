from pymongo import UpdateOne
import logging
from airflow import AirflowException
from pymongo import InsertOne, UpdateOne
from bson.objectid import ObjectId


def insert_or_update_mongodb(mongo_db, collection_name, documents):
    """
    Bulk insert documents into a MongoDB collection.

    Parameters:
    - mongo_db: MongoDB database object.
    - collection_name: Name of the target collection.
    - documents: List of documents to insert.
    - create_indexes: Whether to create indexes on the target collection.
    - dryrun: If True, the function performs a dry run without actual insertion.

    Returns:
    - The number of documents inserted.
    """
    
    if not documents:
        logging.warning("No documents to insert. Skipping insert...")
        return 0

    # check if collection exists
    if collection_name not in mongo_db.list_collection_names():
        logging.info(f"Collection '{collection_name}' does not exist in '{mongo_db.name}'. Creating collection...")
        mongo_db.create_collection(collection_name)
        # create indexes
        mongo_db[collection_name].create_index("participantId")
        mongo_db[collection_name].create_index("attribute")

    target_collection = mongo_db[collection_name]
    bulk_operations = []
    for document in documents:
        # if '_id' in document:
        #     document.pop('_id')
        # bulk_operations.append(InsertOne(document))
        
        # check if _id is ObjectId if not convert it to ObjectId or create a new ObjectId
        if not isinstance(document['_id'], ObjectId):
            try:
                document['_id'] = ObjectId(document['_id'])
            except:
                # create a new ObjectId
                document['_id'] = ObjectId()    #This is done so that _id is always an ObjectId, which will make it easier to copy data into Elasticsearch.
                # do nothing
                # pass
        # bulk_operations.append(UpdateOne({'_id': document['_id']}, {'$set': document}, upsert=True))
        bulk_operations.append(UpdateOne(
    {'_id': document['_id']},
    {'$set': {k: v for k, v in document.items() if k != '_id'}},
    upsert=True
))
    result = target_collection.bulk_write(bulk_operations, ordered=False)
    acknowledged = result.acknowledged
    inserted_count = result.upserted_count
    modified_count = result.modified_count
    upserted_count = result.upserted_count
    logging.info(f"acknowledged: {acknowledged}, inserted_count: {inserted_count}, modified_count: {modified_count}, upserted_count: {upserted_count}")
    logging.info(f"Inserted/Updated {inserted_count + modified_count}/{len(documents)} documents into '{mongo_db.name}.{collection_name}'")

    return 0