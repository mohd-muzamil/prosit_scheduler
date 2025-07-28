import logging
from psycopg2 import sql, extras


def create_db_if_not_exists(conn, db_name):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_database
                WHERE datname = %s
            );
        """, (db_name,))
        db_exists = cursor.fetchone()[0]

        if not db_exists:
            logging.info(f"Database '{db_name}' does not exist. Creating database...")
            cursor.execute(sql.SQL("CREATE DATABASE {db};").format(db=sql.Identifier(db_name)))
            conn.commit()


def create_schema_if_not_exists(conn, db_name, schema_name):
    # connect to db_name and create schema if not exists
    with conn.cursor() as cursor:
        # Set the search path to the desired database
        # cursor.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(db_name)))
        
        # Check for the existence of the schema
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.schemata
                WHERE schema_name = %s
            );
        """, (schema_name,))
        schema_exists = cursor.fetchone()[0]

        if not schema_exists:
            logging.info(f"Schema '{schema_name}' does not exist in database '{db_name}'. Creating schema...")
            cursor.execute(sql.SQL("CREATE SCHEMA {schema};").format(schema=sql.Identifier(schema_name)))
            conn.commit()


def create_table_if_not_exists(conn, db_name, schema_name, table_name, columns):
    with conn.cursor() as cursor:
        # Set the search path to the desired database
        cursor.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(db_name)))

        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            );
        """, (schema_name, table_name))
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Check if table_name is 'location'
            if table_name == 'location':
                # Add the additional column for 'location' table
                column_definitions = ', '.join([f'{col} TEXT' for col in columns] + ['location_decrypted TEXT'])
            else:
                column_definitions = ', '.join([f'{col} TEXT' for col in columns])
                
            create_table_query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    _id TEXT PRIMARY KEY,
                    participantId TEXT NOT NULL,
                    measuredAt TIMESTAMP,
                    uploadedAt TIMESTAMP,
                    {columns}
                );
            """).format(
                schema=sql.Identifier(schema_name),
                table=sql.Identifier(table_name),
                columns=sql.SQL(column_definitions)
            )

            cursor.execute(create_table_query)
            conn.commit()


def insert_or_update_psql(conn, db_name, schema_name, table_name, documents):
    """
    Bulk insert or update records into a PostgreSQL table.

    Parameters:
    - conn: PostgreSQL database connection object.
    - table_name: Name of the target table.
    - documents: List of dictionaries representing records to insert or update.

    Returns:
    - The number of records inserted or updated.
    """
    
    if not documents:
        logging.warning("No documents to insert or update. Skipping operation...")
        return 0

    doc = documents[0]
    if isinstance(doc['value'], list):
        doc_len = len(doc['value'])
        columns = [f'value{i}' for i in range(doc_len)]
    else:
        # Handle cases where doc['value'] is not a list (e.g., it's a float)
        columns = ['value0']
    value_columns = ', '.join([f'{col}' for col in columns])
    update_columns = ', '.join([f'{col} = EXCLUDED.{col}' for col in columns])
    value_placeholder = ', '.join(['%s' for col in columns])

    db_name = db_name.lower()
    schema_name = schema_name.lower()
    table_name = table_name.lower()
    columns = [col.lower() for col in columns]

    create_schema_if_not_exists(conn, db_name, schema_name)
    create_table_if_not_exists(conn, db_name, schema_name, table_name, columns)
    
    # Conditionally add column if table_name is 'location'
    if table_name == 'location':
        # Ensure that 'location_decrypted' column is included in the value columns
        value_columns = f'{value_columns}, location_decrypted'
        update_columns = f'{update_columns}, location_decrypted = EXCLUDED.location_decrypted'
        value_placeholder = f'{value_placeholder}, %s'


    # Prepare SQL statements
    insert_sql = query = f'''
                INSERT INTO {table_name} (_id, participantId, measuredAt, uploadedAt, {value_columns})
                VALUES (%s, %s, %s, %s, {value_placeholder})
                ON CONFLICT (_id)
                DO UPDATE SET
                    participantId = EXCLUDED.participantId,
                    measuredAt = EXCLUDED.measuredAt,
                    uploadedAt = EXCLUDED.uploadedAt,
                    {update_columns}
            '''


    data_tuples = []

    # Convert data to tuples for bulk insert
    for doc in documents:
        if isinstance(doc['value'], list):
            value_args = [str(val) for val in doc['value']]
        else:
            value_args = [str(doc['value'])]

        if table_name == 'location':
            insert_doc = (str(doc['_id']), doc['participantId'], doc['measuredAt'], doc['uploadedAt'], *value_args, doc['location_decrypted'])
        else:
            insert_doc = (str(doc['_id']), doc['participantId'], doc['measuredAt'], doc['uploadedAt'], *value_args)
        data_tuples.append(insert_doc)

    with conn.cursor() as cursor:
        # Set the search path to the desired database
        # connect to db_name and insert data_tuples into table_name
        cursor.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(db_name)))
        # set the search path to the desired schema
        cursor.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name)))
        cursor.executemany(insert_sql, data_tuples)
        
        conn.commit()
        inserted_count = cursor.rowcount
        logging.info(f"Inserted/Updated {inserted_count}/{len(documents)} records into '{db_name}.{schema_name}.{table_name}'")

    return inserted_count
