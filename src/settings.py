import os

DB_URI_OLTP = os.getenv('DB_URI_OLTP', 'sqlite:///db.sqlite3')
DB_ENGINE_OLTP = DB_URI_OLTP.split(':')[0].split('+')[0]

DB_PATH_FOOTBALL_CO_BRONZE = 'football_co_bronze.db'
