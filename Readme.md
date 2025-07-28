<!--  -->
ssh port forwarding from remote to localhost:  
`ssh -N -L localhost:8085:localhost:8080 mmh@loki.research.cs.dal.ca  `

references:  
airflow installation:  
`https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html` 


job1_config = {
    "source_conn_id": "frigg2_mongo",
    "target_conn_id": "loki_mongo",
    "psql_conn_id": "vidar_psql",
    "source_db_name": "prosit",
    "target_db_name": "entries",
    "psql_staging_db_name": "staging_db",
    "source_collection_name": "entries",
    "target_collection_name": "entries_date",
    "dryrun": True,
    "encryption_key_location": "password1",
    "encryption_key_location_new": "password2"
}

<!-- connections ids for db connections: -->
frigg2_mongo: `mongodb://superuser:You_sound_like_a_native_speaker@localhost:8000/admin`  
loki_mongo: `mongodb://root:8h37fjfd63hf9hn3f@localhost:27017/admin?authSource=admin`  
vidar_psql: `postgresql://postgres:6zRqRq4ZckX7k6@localhost/analysis`  


This is a working code and currently in production....Need to test it for the data counts.

<b> Airflow Dags:  </b>
- Job1: Staging PROSIT data from frigg2 mongodb -> loki mongodb -> loki PSQL
- Job2: Automated emails with social media participants with locations outside canada
- Job3: Staging PROSIT data from OP1 mongodb -> loki mongodb -> loki ELK -> loki PSQL

