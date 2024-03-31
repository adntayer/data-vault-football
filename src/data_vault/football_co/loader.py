# python -m src.data_vault.football_co.loader
import os
import time
import warnings
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pandas as pd
import ulid
from settings import DB_PATH_FOOTBALL_CO_BRONZE

from src.logger import SetupLogger

_log = SetupLogger('src.data_vault.football_co.loader')


def load_csv_to_duckdb():
    base_dir = os.path.join('src', 'data', '01-landing', 'football-co')
    list_csv_files = []
    start_time = time.time()

    for country in os.listdir(base_dir):
        country_dir = os.path.join(base_dir, country)
        for season in os.listdir(country_dir):
            season_dir = os.path.join(country_dir, season)
            for csv_file in os.listdir(season_dir):
                if csv_file.endswith('.csv'):
                    league = csv_file[:-4]
                    full_path = os.path.join(season_dir, csv_file)
                    table_name = f'{country}_{season}_{league}'.replace(' ', '_').lower()
                    list_csv_files.append((full_path, table_name, country, season, league))

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(load_csv, *file): file for file in list_csv_files}

    for future in futures:
        try:
            future.result()
        except Exception as e:
            _log.error(f'Thread encountered an error while loading {futures[future]}: {e}')

    end_time = time.time()
    _log.info(f"Loaded {len(list_csv_files)} files in '{end_time - start_time:.0f}' seconds")


def load_csv(full_path, table_name, country, season, league):
    warnings.filterwarnings('ignore')
    ulid_thread = str(ulid.ULID())
    try:
        con = duckdb.connect(DB_PATH_FOOTBALL_CO_BRONZE)
        _log.info(f'[{country}] [{season}] [{league}] ({ulid_thread}) | Loading from {full_path}')

        df = pd.read_csv(full_path, sep=',', encoding='utf-8', index_col=False)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], dayfirst=True)

        _log.error(f'[{country}] [{season}] [{league}] ({ulid_thread}) | Creating table')
        con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        con.execute(f"""CREATE TABLE "{table_name}" AS SELECT * FROM df""")
        _log.error(f'[{country}] [{season}] [{league}] ({ulid_thread}) | Table created')

        if 'country' not in df.columns:
            _log.error(f'[{country}] [{season}] [{league}] ({ulid_thread}) | Creating country')
            con.execute(f'ALTER TABLE "{table_name}" ADD COLUMN country TEXT')
            con.execute(f"UPDATE '{table_name}' SET country = '{country}'")
            _log.error(f'[{country}] [{season}] [{league}] ({ulid_thread}) | Country created')

        if 'season' not in df.columns:
            _log.error(f'[{country}] [{season}] [{league}] ({ulid_thread}) | Creating season')
            con.execute(f'ALTER TABLE "{table_name}" ADD COLUMN season TEXT')
            con.execute(f"UPDATE '{table_name}' SET season = '{season}'")
            _log.error(f'[{country}] [{season}] [{league}] ({ulid_thread}) | Season creatd')

        if 'league' not in df.columns:
            _log.error(f'[{country}] [{season}] [{league}] ({ulid_thread}) | Creating league')
            con.execute(f'ALTER TABLE "{table_name}" ADD COLUMN league TEXT')
            con.execute(f"UPDATE '{table_name}' SET league = '{league}'")
            _log.error(f'[{country}] [{season}] [{league}] ({ulid_thread}) | League created')

        _log.error(f'[{country}] [{season}] [{league}] ({ulid_thread}) | Loaded')

    except Exception as e:
        _log.error(f'[{country}] [{season}] [{league}] ({ulid_thread}) | Error loading from {full_path}: {e}')
    finally:
        con.close()


if __name__ == '__main__':
    load_csv_to_duckdb()
