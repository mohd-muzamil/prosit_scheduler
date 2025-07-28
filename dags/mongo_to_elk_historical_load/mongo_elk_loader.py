from mongo_to_elk_historical_load import *
from airflow.models import Variable


# Initialize Redis client
redis_client = redis.Redis(host="prosit_scheduler_redis_1", port=6379, db=0)

def decrypt_value_column(enc_data: str, secret_key: str) -> Union[str, None]:
    """Decrypt value using AES-CBC with the provided secret key."""
    try:
        enc_dict = json.loads(enc_data)
        iv = bytes.fromhex(enc_dict['iv'])
        content = bytes.fromhex(enc_dict['content'])

        cipher = Cipher(algorithms.AES(bytes.fromhex(secret_key)), 
                     modes.CBC(iv), 
                     backend=default_backend())
        decryptor = cipher.decryptor()

        decrypted_padded = decryptor.update(content) + decryptor.finalize()
        padding_length = decrypted_padded[-1]

        if padding_length > 16:
            logging.error(f"Invalid padding length: {padding_length}")
            return None

        return decrypted_padded[:-padding_length].decode('utf-8')
    except Exception as e:
        logging.error(f"Error decrypting data: {e}")
        return None

def decrypt_location_data(loc: Union[str, list], k: str) -> Union[str, None]:
    """Decrypt location data using the provided key."""
    try:
        if not loc:
            return None
            
        loc_str = str(loc[0]) if isinstance(loc, list) else str(loc)
        if not loc_str:
            return None
            
        return rn.decrypt(base64.b64decode(loc_str), k)
    except Exception as e:
        # logging.error(f"Error decrypting location data: {e}")
        return None

def process_location(location: str, secret_key: str, encryption_keys: list, 
                    redis_client: redis.Redis = None) -> Union[str, None]:
    """
    Process location data with two-layer Redis caching and automatic expiration.
    
    Layer 1: Encrypted input → Normalized location (with TTL)
    Layer 2: Normalized location → Standard form (long-term cache)
    
    Args:
        location: Encrypted location string
        secret_key: Key for initial decryption (OP1 studies only)
        encryption_keys: List of keys for final location decryption
        redis_client: Optional Redis connection
    
    Returns:
        Decrypted and normalized location, or None if decryption fails
    """
    if not location:
        return None

    # Constants
    CACHE_TTL = 30 * 24 * 60 * 60  # 30 days in seconds
    CACHE_PREFIX_ENC = "loc_enc:"   # Layer 1: Encrypted → Normalized
    CACHE_PREFIX_NORM = "loc_norm:" # Layer 2: Normalized → Standard
    
    # Layer 1: Check encrypted → normalized cache
    enc_key = f"{CACHE_PREFIX_ENC}{md5(location.encode()).hexdigest()}"
    
    if redis_client:
        # Try to get cached normalized location
        if (cached := redis_client.get(enc_key)):
            return cached.decode('utf-8')

    # Decryption process
    result = None
    for k in encryption_keys:
        try:
            # OP1 studies need two-stage decryption
            if secret_key:
                intermediate = decrypt_value_column(location, secret_key)
                if not intermediate:
                    continue
                location_decrypted = decrypt_location_data(intermediate, k)
            else:
                # Regular studies decrypt directly
                location_decrypted = decrypt_location_data(location, k)
            
            if location_decrypted:
                # Normalize the result (e.g., "New York, USA" → "USA")
                result = location_decrypted.split(',')[-1].strip()
                # if 'None' in results then result is None
                if result.lower() == 'none':
                    result = None
                break
                
        except Exception as e:
            logging.debug(f"Decryption failed with key {k[-6:]}: {e}")
            continue

    if not result:
        # logging.warning("Location decryption failed for all keys")
        return None

    # Layer 2: Check normalized → standard cache
    norm_key = f"{CACHE_PREFIX_NORM}{md5(result.encode()).hexdigest()}"
    
    if redis_client:
        with redis_client.pipeline() as pipe:
            # Store both cache layers atomically
            pipe.setex(enc_key, CACHE_TTL, result)  # Layer 1 with TTL
            pipe.set(norm_key, result)              # Layer 2 permanent
            pipe.execute()
            
            # Check if Layer 2 had a more recent value
            if (final_result := redis_client.get(norm_key)):
                return final_result.decode('utf-8')

    return result

def serialize_document(doc: dict, db_name: str, secret_key: str, 
                      encryption_keys: list, redis_client: redis.Redis = None) -> dict:
    """Prepare document for Elasticsearch indexing with proper decryption."""
    try:
        # Convert special types to serializable formats
        if isinstance(doc['_id'], ObjectId):
            doc['_id'] = str(doc['_id'])
        for date_field in ['measuredAt', 'uploadedAt']:
            if isinstance(doc.get(date_field), datetime):
                doc[date_field] = doc[date_field].isoformat()

        # Handle value field based on attribute type
        if 'value' in doc:
            attr = doc.get('attribute', '').lower()
            
            # Location handling
            if 'location' in attr:
                doc['value'] = process_location(
                    doc['value'],
                    secret_key if db_name.startswith('op1_study_') else None,
                    encryption_keys,
                    redis_client
                )
            # Other encrypted fields for OP1 studies
            elif db_name.startswith('op1_study_'):
                decrypt_attrs = ['device', 'systeminfo', 'analytics', 'reachability', 'connectivity']
                if any(a in attr for a in decrypt_attrs):
                    doc['value'] = decrypt_value_column(doc['value'], secret_key)
                else:
                    doc.pop('value', None)
            # For regular studies, leave value as-is if not location
            
        return doc
    except Exception as e:
        logging.error(f"Error serializing doc {doc.get('_id')}: {e}")
        return None

def copy_documents_from_mongodb_to_elastic_search(config: dict) -> int:
    """
    Main function to transfer data from MongoDB to Elasticsearch
    with progress logging and configurable database filters.
    """
    source_mongo = MongoHook(mongo_conn_id=config["source_conn_id"])
    target_es = ElasticsearchHook(es_conn_id=config["target_conn_id"])
    es_conn = target_es.get_conn()

    secret_key = config["secret_key"]
    encryption_keys = config.get("encryption_keys", [])
    dryrun = config.get("dryrun", True)
    batch_size = config.get("batch_size", 1000)
    max_workers = config.get("max_workers", 4)
    allowed_dbs_filter = config.get("allowed_dbs")

    time_filter = {}
    if config.get("start_date_time"):
        time_filter.setdefault("uploadedAt", {})["$gt"] = config["start_date_time"]
    if config.get("end_date_time"):
        time_filter.setdefault("uploadedAt", {})["$lte"] = config["end_date_time"]

    db_names = [db for db in source_mongo.list_databases()
                if db not in ["admin", "local", "config", "study_staging"]]

    if isinstance(allowed_dbs_filter, list):
        allowed_dbs = [db for db in db_names if db in allowed_dbs_filter]
    elif isinstance(allowed_dbs_filter, str):
        allowed_dbs = [db for db in db_names if db.startswith(allowed_dbs_filter)]
    else:
        allowed_dbs = db_names

    total_inserted = 0
    created_indices = set()
    index_creation_lock = threading.Lock()
    progress_counter = {"current": 0, "total": 0}
    progress_lock = threading.Lock()

    # Precompute all collections to get accurate progress count
    all_collections = []
    for db_name in allowed_dbs:
        try:
            source_db = source_mongo.get_database(db_name)
            collections = [col for col in source_db.list_collection_names()
                           if source_db[col].count_documents(time_filter) > 0]
            for col_name in collections:
                all_collections.append((db_name, col_name))
        except Exception as e:
            logging.error(f"Could not process db {db_name}: {e}")
            continue

    progress_counter["total"] = len(all_collections)

    def process_collection(db_name: str, col_name: str, thread_label: str) -> int:
        nonlocal total_inserted
        target_index = db_name
        source_db = source_mongo.get_database(db_name)

        with progress_lock:
            progress_counter["current"] += 1
            current_i = progress_counter["current"]
            total_i = progress_counter["total"]

        try:
            total_docs = source_db[col_name].count_documents(time_filter)
            logging.info(f"[{current_i}/{total_i}] [{thread_label}] Starting: {db_name}.{col_name} → {target_index} | {total_docs} docs")
        except Exception as e:
            logging.error(f"[{thread_label}] Error counting docs: {e}")
            return 0

        if dryrun:
            logging.info(f"[{thread_label}] DRYRUN: Would insert {total_docs} docs into {target_index}")
            return total_docs

        start_time = time.time()

        with index_creation_lock:
            if target_index not in created_indices:
                try:
                    if not es_conn.indices.exists(index=target_index):
                        es_conn.indices.create(index=target_index)
                        logging.info(f"[{thread_label}] Created index: {target_index}")
                    created_indices.add(target_index)
                except Exception as e:
                    logging.error(f"[{thread_label}] Failed to create index {target_index}: {e}")
                    return 0

        def generate_actions():
            cursor = source_db[col_name].find(time_filter).batch_size(batch_size)
            for doc in cursor:
                processed = serialize_document(doc, db_name, secret_key, encryption_keys, redis_client)
                if processed:
                    yield {
                        "_op_type": "index",
                        "_index": target_index,
                        "_id": processed['_id'],
                        "_source": {k: v for k, v in processed.items() if k != '_id'}
                    }

        try:
            success, failed = bulk(
                client=es_conn,
                actions=generate_actions(),
                chunk_size=batch_size,
                raise_on_error=False
            )
            elapsed = round(time.time() - start_time, 2)
            logging.info(f"[{thread_label}] Finished: {success} inserted, {len(failed)} failed, took {elapsed}s")
            if failed:
                logging.warning(f"[{thread_label}] Failed documents: {len(failed)}")
            es_conn.indices.refresh(index=target_index)
            return success
        except Exception as e:
            logging.error(f"[{thread_label}] Bulk insert failed: {e}")
            return 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for db_name, col_name in all_collections:
            thread_label = f"{db_name}.{col_name}"
            futures.append(executor.submit(process_collection, db_name, col_name, thread_label))

        for future in as_completed(futures):
            try:
                total_inserted += future.result() or 0
            except Exception as e:
                logging.error(f"Error in thread execution: {e}")

    logging.info(f"All collections processed. Total inserted documents: {total_inserted}")
    source_mongo.close_conn()
    target_es.close_conn()
    return total_inserted