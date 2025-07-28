# __init__.py
# imports
import os
from pathlib import Path
import re
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import rncryptor as rn
import base64
from hashlib import md5
from pymongo import UpdateOne, InsertOne
import psycopg2
import shutil
import subprocess
from typing import Union
import time

from elasticsearch import helpers
from elasticsearch.helpers import bulk

# helper functions
from helper_functions.sqlite_hook import SQLiteHook
from helper_functions.mongo_hook import MongoHook
from helper_functions.mongo_operations import insert_or_update_mongodb
from helper_functions.elasticsearch_hook import ElasticsearchHook
from helper_functions.psql_hook import PsqlHook
from helper_functions.psql_operations import insert_or_update_psql

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator


import logging

# Suppress Elasticsearch client logs while retaining others
elasticsearch_logger = logging.getLogger("elasticsearch")
elasticsearch_logger.setLevel(logging.WARNING)  # Suppress INFO logs from Elasticsearch client