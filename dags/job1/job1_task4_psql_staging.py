'''
Description: This script will copy the documents from mongodb to psql
Loki mongoDB entries -> Vidar psqlDB staging_db
'''
from job1 import *

# Sample decryption function (replace with your actual decryption function)
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

def copy_documents_from_mongodb_to_psql(config: dict) -> None:
    '''
    Copy data from one MongoDB collection to PSQL database
    config: dictionary with source and target db connection details
    '''
    source_conn_id = config["source_conn_id"]
    target_conn_id = config["target_conn_id"]
    target_db_name = config["target_db_name"]
    encryption_key_location_new = config["encryption_key_location_new"]
    start_date_time = config["start_date_time"],
    end_date_time = config["end_date_time"],
    dryrun = config["dryrun"]

    source_mongo_hook = MongoHook(mongo_conn_id=source_conn_id)
    source_conn = source_mongo_hook.get_conn()
    target_psql_hook = PsqlHook(psql_conn_id=target_conn_id)
    target_conn = target_psql_hook.get_conn()

    logging.info(f"Copying data from study_staging.all to '{target_db_name}' for datetime: {start_date_time} to {end_date_time}  ")

    # Create a PyMongo query (your actual query here)
    time_filter = {
        "uploadedAt": {
            "$gt": start_date_time[0],
            "$lte": end_date_time[0]
        }
    }

    # just need to copy the documents from the study_staging db to psql
    source_db_name = 'study_staging'
    source_collection_name = 'all'
    source_db = source_conn[source_db_name]
    # fetching the documents from the source db
    documents_to_copy = list(source_db[source_collection_name].find(time_filter))
    if len(documents_to_copy) == 0:
        logging.info(f"No documents found for datetime: {start_date_time} to {end_date_time} to copy into psql.")
        return 0
    else:
        logging.info(f"Inserting or updating {len(documents_to_copy)} documents into '{target_db_name}'")
        df = pd.DataFrame(documents_to_copy)
        studies = df['study'].unique()
        for study in studies:
            target_schema_name = "study_" + study
            df_study = df[df['study'] == study]
            # drop the study column
            df_study = df_study.drop(columns=['study'])
            
            distinct_attributes = df_study['attribute'].unique()
            for attribute in distinct_attributes:
                target_table_name = attribute.lower().replace(' ', '_')
                
                df_attribute = df_study[df_study['attribute'] == attribute].copy()
                # Make sure to use .copy() if df_attribute is derived from another DataFrame

                # check if target_table_name is location, if yes we need to add another column called location_decrypted
                if target_table_name == 'location':
                    df_attribute.loc[:, 'location_decrypted'] = df_attribute['value'].apply(decrypt_location_data, k=encryption_key_location_new)

                # insert the data into psql if dryrun is False
                if dryrun:
                    logging.info(f"DRYRUN: Would have copied {len(df_attribute)} documents from '{source_db_name}'.'{source_collection_name}' to '{target_db_name}'.'{target_schema_name}'.'{target_table_name}' for datetime: {start_date_time} to {end_date_time}. skipping the copy operation.")
                else:
                    insert_or_update_psql(conn=target_conn, db_name=target_db_name, schema_name=target_schema_name, table_name=target_table_name, documents=df_attribute.to_dict(orient='records'))
                    logging.info(f"{len(df_attribute)} documents copied from study_staging.all to psql table '{target_db_name}'.'{target_schema_name}'.'{attribute.lower().replace(' ', '_')}'")

    logging.info(f"Documents copied from staging__db mongodb to psql")
    source_mongo_hook.close_conn()
    target_conn.close()
    