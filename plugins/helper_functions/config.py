from configparser import ConfigParser
from pathlib import Path

config = ConfigParser()
file = f"{Path.home()}/prosit-server.config"
try:
    config.read(file)
except Exception as e:
    print(f"Config file missing at user home folder: {file}.")
