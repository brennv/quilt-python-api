import json
import requests
import sys

HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
QUILT_URL = 'https://quiltdata.com'

try:
    import pandas as pd
    PANDAS = True
except:
    PANDAS = False

try:
    import psycopg2
    import sqlalchemy as sa
    SQLALCHEMY = True
except:
    SQLALCHEMY = False
