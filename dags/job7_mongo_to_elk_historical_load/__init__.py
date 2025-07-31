# mongo_to_elk_historical_load/__init__.py
import logging
import os
from pathlib import Path
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
from elasticsearch.helpers import bulk
from bson import ObjectId
import redis
import json

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

from helper_functions.sqlite_hook import SQLiteHook
from helper_functions.mongo_hook import MongoHook
from helper_functions.elasticsearch_hook import ElasticsearchHook
from helper_functions.mongo_operations import insert_or_update_mongodb
from helper_functions.psql_hook import PsqlHook
from helper_functions.psql_operations import insert_or_update_psql

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
