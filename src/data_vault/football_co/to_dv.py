# python -m src.data_vault.football_co.to_dv
import logging
import os
import time
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import duckdb
import pandas as pd
import psutil
import ulid

from src.data_vault.football_co.fixtures import DICT_MATCH_RESULTS
from src.data_vault.football_co.fixtures import DICT_MATCH_STATISTICS
from src.data_vault.football_co.fixtures import DICT_ODDS_SPORTINGBET
from src.data_vault.football_co.loader import DB_PATH_FOOTBALL_CO_BRONZE
from src.db.hashes import create_hash
from src.db.io_utils import load_to_table
from src.db.models.dv.hub_leagues import HubLeagues
from src.db.models.dv.hub_matches import HubMatches
from src.db.models.dv.hub_seasons import HubSeasons
from src.db.models.dv.hub_teams import HubTeamns
from src.db.models.dv.lnk_matches import LnkMatches
from src.db.models.dv.sat_matches_core_results import SatMatchesCoreResults
from src.db.models.dv.sat_matches_core_statistics import SatMatchesCoreStatistics
from src.db.models.dv.sat_matches_odds_bet365 import SatMatchesOddsBet365
from src.db.models.dv.sat_matches_odds_market_consensus import SatMatchesOddsMarketConsensus
from src.db.models.dv.sat_matches_odds_sportingbet import SatMatchesOddsSportingbet
from src.db.track_logs import log_error
from src.db.track_logs import log_start
from src.db.track_logs import log_success
from src.logger import SetupLogger

_log = SetupLogger('src.data_vault.football_co.to_dv')
PATH_CORE_DV = os.path.join('src', 'data', '02-dv')
list_models = [
    HubLeagues,
    HubMatches,
    HubSeasons,
    HubTeamns,
    LnkMatches,
    SatMatchesCoreResults,
    SatMatchesCoreStatistics,
    SatMatchesOddsMarketConsensus,
    SatMatchesOddsBet365,
    SatMatchesOddsSportingbet,
]


def main():
    con = duckdb.connect(DB_PATH_FOOTBALL_CO_BRONZE)
    df_tables = con.sql('show all tables').df()
    list_tables = df_tables['name'].tolist()
    _log.info(f'found {len(list_tables):4} tables')

    for model in list_models:
        path_model = os.path.join(PATH_CORE_DV, model.__table__.name)
        os.makedirs(path_model, exist_ok=True)

    with ThreadPoolExecutor() as executor:
        futures = []

        for e, table in enumerate(list_tables, 1):
            _log.info(f'{e:4}/{len(list_tables):4} | processing {table}')
            df_table = con.sql(f'select * from {table}').df()
            df_table['src'] = 'football-co'
            df_table['src_record'] = table

            futures.append(executor.submit(process_and_save_local, df_table))
        con.close()

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                _log.error(f'Error processing: {e}')

    batch_load_database()


def setup_process_logger(ulid_thread):
    formatting = '[%(levelname)7s - %(asctime)s] [%(filename)s:%(name)s:%(funcName)s:%(lineno)d] | %(message)s'
    logging.basicConfig(format=formatting)
    process_logger = logging.getLogger(f'process_and_save_local_{ulid_thread}')
    process_logger.setLevel(logging.INFO)

    if not process_logger.handlers:
        if not os.path.exists('logs'):
            os.makedirs('logs')

        formatter = logging.Formatter('[%(levelname)7s - %(asctime)s] | %(message)s')

        file_handler = logging.FileHandler(os.path.join('logs', f'process_and_save_local_{ulid_thread}.log'))
        file_handler.setFormatter(formatter)
        process_logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        process_logger.addHandler(console_handler)

    return process_logger


def process_and_save_local(df):
    ulid_thread = str(ulid.ULID())
    _log = setup_process_logger(ulid_thread)

    df.columns = df.columns.str.lower()

    source = 'football co - to_dv - process_and_save_local'
    process = f"{df['country'].iloc[0]} - {df['season'].iloc[0]} - {df['league'].iloc[0]}"

    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss
    start_cpu_time = time.process_time()

    log_start(ulid_thread=ulid_thread, source=source, process=process)
    try:
        _log.info(f'({ulid_thread}) | data massage')
        dict_rename = {
            'home': 'home_team',
            'hometeam': 'home_team',
            'HomeTeam': 'home_team',
            'HT': 'home_team',
            'ht': 'home_team',
            'away': 'away_team',
            'awayteam': 'away_team',
            'AwayTeam': 'away_team',
            'AT': 'away_team',
            'at': 'away_team',
            'hg': 'fthg',
            'fthg': 'fthg',
            'ftag': 'ftag',
            'ag': 'ftag',
            'res': 'ftr',
        }
        df.rename(columns=dict_rename, inplace=True)

        df['home_team'] = df['home_team'].str.lower() + ' - ' + df['country'].str.lower()
        df['away_team'] = df['away_team'].str.lower() + ' - ' + df['country'].str.lower()
        df['hk_home_team'] = df['home_team'].apply(lambda row: create_hash(row))
        df['hk_away_team'] = df['away_team'].apply(lambda row: create_hash(row))
        df['hk_league'] = df['league'].apply(lambda row: create_hash(row))
        df['hk_season'] = df['season'].apply(lambda row: create_hash(row))
        df['hk_country'] = df['country'].apply(lambda row: create_hash(row))
        list_cols_hk = ['country', 'league', 'season', 'home_team', 'away_team', 'date', 'time']
        df['hk_match'] = df.reindex(columns=list_cols_hk).fillna('-').astype(str).agg(';'.join, axis=1).apply(lambda row: create_hash(row))

        _log.info(f'({ulid_thread}) | [1/4] Hub  - Teams')
        df_hub_team = df[['hk_home_team', 'home_team', 'hk_away_team', 'away_team', 'src', 'src_record']].copy()
        dict_rename = {'hk_home_team': 'hk', 'home_team': 'bk', 'hk_away_team': 'hk', 'away_team': 'bk', 'src': 'src', 'src_record': 'src_record'}
        df_hub_team = pd.concat(
            [
                df_hub_team[['hk_home_team', 'home_team', 'src', 'src_record']].rename(columns=dict_rename),
                df_hub_team[['hk_away_team', 'away_team', 'src', 'src_record']].rename(columns=dict_rename),
            ],
            ignore_index=True,
        )
        df_hub_team.to_csv(os.path.join(PATH_CORE_DV, HubTeamns.__table__.name, f'{ulid_thread}.csv'), index=False)

        _log.info(f'({ulid_thread}) | [2/4] Hub  - Leagues')
        df_hub_league = df[['hk_league', 'league', 'src', 'src_record']].rename(columns={'hk_league': 'hk', 'league': 'bk', 'src': 'src'}).drop_duplicates()
        df_hub_league.to_csv(os.path.join(PATH_CORE_DV, HubLeagues.__table__.name, f'{ulid_thread}.csv'), index=False)

        _log.info(f'({ulid_thread}) | [3/4] Hub  - Season')
        df_hub_season = df[['hk_season', 'season', 'src', 'src_record']].rename(columns={'hk_season': 'hk', 'season': 'bk', 'src': 'src', 'src_record': 'src_record'}).drop_duplicates()

        def normalize_season(season):
            season = str(season)
            if '/' in season:
                year1, year2 = season.split('/')
                if len(year1) == 2:
                    year1 = datetime.strptime(year1, '%y').year
                if len(year2) == 2:
                    year2 = datetime.strptime(year2, '%y').year

            if '_' in season:
                year1, year2 = season.split('_')
                if len(year1) == 2:
                    year1 = datetime.strptime(year1, '%y').year
                if len(year2) == 2:
                    year2 = datetime.strptime(year2, '%y').year

            if len(season) == 2:
                year1 = datetime.strptime(season, '%y').year
                year2 = datetime.strptime(season, '%y').year

            if len(season) == 4:
                year1 = season
                year2 = season

            return f'{year1}-{year2}'

        df_hub_season['bk'] = df_hub_season['bk'].apply(normalize_season)
        df_hub_season.to_csv(os.path.join(PATH_CORE_DV, HubSeasons.__table__.name, f'{ulid_thread}.csv'), index=False)

        _log.info(f'({ulid_thread}) | [3/4] Hub  - Matches')
        df_hub_match = df[['hk_match', 'country', 'league', 'season', 'home_team', 'away_team', 'date', 'src', 'src_record']]
        dict_rename = {
            'hk_match': 'hk',
            'country': 'bk_match_country',
            'league': 'bk_match_league',
            'season': 'bk_match_season',
            'home_team': 'bk_match_home_team',
            'away_team': 'bk_match_away_team',
            'date': 'bk_match_date',
            'src': 'src',
            'src_record': 'src_record',
        }
        df_hub_match = df_hub_match.rename(columns=dict_rename)
        df_hub_match.to_csv(os.path.join(PATH_CORE_DV, HubMatches.__table__.name, f'{ulid_thread}.csv'), index=False)

        _log.info(f'({ulid_thread}) | [1/1] Lnk - LnkMatches')
        df_lnk_matches = df.copy()
        df_lnk_matches['hk'] = df_lnk_matches['hk_match']
        df_lnk_matches['hk_match_home_team'] = df_lnk_matches['hk_home_team']
        df_lnk_matches['hk_match_away_team'] = df_lnk_matches['hk_away_team']
        df_lnk_matches['hk_match_league'] = df_lnk_matches['hk_league']
        df_lnk_matches['hk_match_season'] = df_lnk_matches['hk_season']
        df_lnk_matches['hk_match_country'] = df_lnk_matches['hk_country']
        df_lnk_matches['src'] = df_lnk_matches['src']
        df_lnk_matches['src_record'] = df_lnk_matches['src_record']
        df_lnk_matches = df_lnk_matches.reindex(columns=[c.name for c in LnkMatches.__table__.columns])
        df_lnk_matches.to_csv(os.path.join(PATH_CORE_DV, LnkMatches.__table__.name, f'{ulid_thread}.csv'), index=False)

        _log.info(f'({ulid_thread}) | [1/5] Sat - SatCoreResults')
        columns_match_core = ['hk_match'] + list(DICT_MATCH_RESULTS.keys())
        columns_match_core = [c.lower() for c in columns_match_core]
        df_sat_match_core_results = df.reindex(columns=columns_match_core + ['src', 'src_record'])
        df_sat_match_core_results['hash_diff'] = df_sat_match_core_results[columns_match_core].apply(lambda row: create_hash(row), axis=1)
        df_sat_match_core_results.rename(columns={'hk_match': 'hk_hub'}, inplace=True)
        df_sat_match_core_results.columns = [col.lower() for col in df_sat_match_core_results.columns]
        df_sat_match_core_results.dropna(subset=['fthg'], inplace=True)
        df_sat_match_core_results.to_csv(os.path.join(PATH_CORE_DV, SatMatchesCoreResults.__table__.name, f'{ulid_thread}.csv'), index=False)

        _log.info(f'({ulid_thread}) | [2/5] Sat - SatCoreStatistics')
        columns_match_core = ['hk_match'] + list(DICT_MATCH_STATISTICS.keys())
        columns_match_core = [c.lower() for c in columns_match_core]
        df_sat_match_core_statistics = df.reindex(columns=columns_match_core + ['src', 'src_record'])
        df_sat_match_core_statistics['hash_diff'] = df_sat_match_core_statistics[columns_match_core].apply(lambda row: create_hash(row), axis=1)
        df_sat_match_core_statistics.rename(columns={'hk_match': 'hk_hub', **DICT_MATCH_STATISTICS}, inplace=True)
        df_sat_match_core_statistics.columns = [col.lower() for col in df_sat_match_core_statistics.columns]
        df_sat_match_core_statistics.to_csv(os.path.join(PATH_CORE_DV, SatMatchesCoreStatistics.__table__.name, f'{ulid_thread}.csv'), index=False)

        _log.info(f'({ulid_thread}) | [3/5] Sat - SatMatchesOddsMarketConsensus')
        dict_map = {
            'maxh': 'maxh',
            'maxch': 'maxch',
            'maxd': 'maxd',
            'maxcd': 'maxcd',
            'maxa': 'maxa',
            'maxca': 'maxca',
            'avgh': 'avgh',
            'avgch': 'avgch',
            'avgd': 'avgd',
            'avgcd': 'avgcd',
            'avga': 'avga',
            'avgca': 'avgca',
            'max>2.5': 'max_over_2_5',
            'maxc>2.5': 'max_c_over_2_5',
            'max<2.5': 'max_under_2_5',
            'maxc<2.5': 'max_c_under_2_5',
            'avg>2.5': 'avg_over_2_5',
            'avgc>2.5': 'avg_c_over_2_5',
            'avg<2.5': 'avg_under_2_5',
            'avgc<2.5': 'avg_c_under_2_5',
        }
        list_odds = list(map(lambda x: x.lower(), dict_map.keys()))
        df_sat_match_odds_market_consensus = df.reindex(columns=['hk_match'] + list_odds + ['src', 'src_record'])
        df_sat_match_odds_market_consensus['hash_diff'] = df_sat_match_odds_market_consensus[list_odds].apply(lambda row: create_hash(row), axis=1)
        df_sat_match_odds_market_consensus.rename(columns={'hk_match': 'hk_hub', **dict_map}, inplace=True)
        df_sat_match_odds_market_consensus.columns = [col.lower() for col in df_sat_match_odds_market_consensus.columns]
        df_sat_match_odds_market_consensus.to_csv(os.path.join(PATH_CORE_DV, SatMatchesOddsMarketConsensus.__table__.name, f'{ulid_thread}.csv'), index=False)

        _log.info(f'({ulid_thread}) | [4/5] Sat - SatMatchesOddsBet365')
        dict_map = {
            'b365h': 'b365h',
            'b365ch': 'b365ch',
            'b365d': 'b365d',
            'b365cd': 'b365cd',
            'b365a': 'b365a',
            'b365ca': 'b365ca',
            'b365>2.5': 'b365_over_2_5',
            'b365<2.5': 'b365_under_2_5',
        }
        list_odds = list(map(lambda x: x.lower(), dict_map.keys()))
        df_sat_match_odds_bet365 = df.reindex(columns=['hk_match'] + list_odds + ['src', 'src_record'])
        df_sat_match_odds_bet365['hash_diff'] = df_sat_match_odds_bet365[list_odds].apply(lambda row: create_hash(row), axis=1)
        df_sat_match_odds_bet365.rename(columns={'hk_match': 'hk_hub', **dict_map}, inplace=True)
        df_sat_match_odds_bet365.columns = [col.lower() for col in df_sat_match_odds_bet365.columns]
        df_sat_match_odds_bet365.to_csv(os.path.join(PATH_CORE_DV, SatMatchesOddsBet365.__table__.name, f'{ulid_thread}.csv'), index=False)

        _log.info(f'({ulid_thread}) | [5/5] Sat - SatMatchesOddsSportingbet')
        list_odds = list(map(lambda x: x.lower(), DICT_ODDS_SPORTINGBET.keys()))
        df_sat_match_odds_sportingbet = df.reindex(columns=['hk_match'] + list_odds + ['src', 'src_record'])
        df_sat_match_odds_sportingbet['hash_diff'] = df_sat_match_odds_sportingbet[list_odds].apply(lambda row: create_hash(row), axis=1)
        df_sat_match_odds_sportingbet.rename(columns={'hk_match': 'hk_hub', **dict_map}, inplace=True)
        df_sat_match_odds_sportingbet.columns = [col.lower() for col in df_sat_match_odds_sportingbet.columns]
        df_sat_match_odds_sportingbet.to_csv(os.path.join(PATH_CORE_DV, SatMatchesOddsSportingbet.__table__.name, f'{ulid_thread}.csv'), index=False)

        ### REPET HERE FOR EACH 'N' BETs
        ### ....

        log_success(ulid_thread=ulid_thread, start_time=start_time, start_memory=start_memory, start_cpu_time=start_cpu_time)
    except Exception as e:
        error_message = str(e)
        _log.error(f'Error processing ({ulid_thread}): {error_message}')
        log_error(ulid_thread=ulid_thread, error_message=error_message, start_time=start_time, start_memory=start_memory, start_cpu_time=start_cpu_time)


def batch_load_database():
    dict_model = {
        'hub_teams': HubTeamns,
        'hub_leagues': HubLeagues,
        'hub_seasons': HubSeasons,
        'hub_matches': HubMatches,
        'lnk_matches': LnkMatches,
        'sat_matches_core_results': SatMatchesCoreResults,
        'sat_matches_core_statistics': SatMatchesCoreStatistics,
        'sat_matches_odds_market_consensus': SatMatchesOddsMarketConsensus,
        'sat_matches_odds_bet365': SatMatchesOddsBet365,
        'sat_matches_odds_sportingbet': SatMatchesOddsSportingbet,
    }
    dict_method = {
        'hub_teams': 'on_conflict_do_nothing',
        'hub_leagues': 'on_conflict_do_nothing',
        'hub_seasons': 'on_conflict_do_nothing',
        'hub_matches': 'on_conflict_do_nothing',
        'lnk_matches': 'on_conflict_do_nothing',
        'sat_matches_core_results': 'compare_hashdiff',
        'sat_matches_core_statistics': 'compare_hashdiff',
        'sat_matches_odds_market_consensus': 'compare_hashdiff',
        'sat_matches_odds_bet365': 'compare_hashdiff',
        'sat_matches_odds_sportingbet': 'compare_hashdiff',
    }

    # the order matters // 1st hub // 2nd link // 3th sat
    for model_folder in sorted(os.listdir(PATH_CORE_DV)):
        if model_folder in dict_model:
            ulid_thread = str(ulid.ULID())
            _log = setup_process_logger(ulid_thread)  # Set up logger with ULID
            source = 'football co - to_dv - batch_load_database'
            start_time = time.time()
            start_memory = psutil.Process().memory_info().rss
            start_cpu_time = time.process_time()
            con = duckdb.connect(DB_PATH_FOOTBALL_CO_BRONZE)
            df = con.execute(f"SELECT * FROM '{os.path.join(PATH_CORE_DV, model_folder)}/*.csv';").df()
            _log.info(f'{model_folder}: {df.shape}')
            method = dict_method[model_folder]
            model = dict_model[model_folder]
            process = model_folder
            log_start(ulid_thread=ulid_thread, source=source, process=process)
            try:
                load_to_table(df=df, model=model, method=method, id_log=ulid_thread)
                log_success(ulid_thread=ulid_thread, start_time=start_time, start_memory=start_memory, start_cpu_time=start_cpu_time)
            except Exception as e:
                error_message = str(e)
                _log.error(f'Error processing ({ulid_thread}): {error_message}')
                log_error(ulid_thread=ulid_thread, error_message=error_message, start_time=start_time, start_memory=start_memory, start_cpu_time=start_cpu_time)


if __name__ == '__main__':
    # python -m src.data_vault.football_co.to_dv
    main()
    # batch_load_database()
