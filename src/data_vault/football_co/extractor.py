# python -m src.data_vault.football_co.extractor
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import requests
import ulid

from src.logger import SetupLogger

_log = SetupLogger('src.data_vault.football_co.extractor')

dict_div = {
    'E0': {'country': 'England', 'league': 'Premier League'},
    'E1': {'country': 'England', 'league': 'Championship'},
    'E2': {'country': 'England', 'league': 'League 1'},
    'E3': {'country': 'England', 'league': 'League 2'},
    'E4': {'country': 'England', 'league': 'Conference'},
    'SC0': {'country': 'Scotland', 'league': 'Premier League'},
    'SC1': {'country': 'Scotland', 'league': 'Division 1'},
    'SC2': {'country': 'Scotland', 'league': 'Division 2'},
    'SC3': {'country': 'Scotland', 'league': 'Division 3'},
    'D1': {'country': 'Germany', 'league': 'Bundesliga 1'},
    'I1': {'country': 'Italy', 'league': 'Serie A'},
    'I2': {'country': 'Italy', 'league': 'Serie B'},
    'SP1': {'country': 'Spain', 'league': 'La Liga Primera Division'},
    'SP2': {'country': 'Spain', 'league': 'La Liga Segunda Division'},
    'F1': {'country': 'France', 'league': 'Le Championnat'},
    'F2': {'country': 'France', 'league': 'Division 2'},
    'N1': {'country': 'Netherlands', 'league': 'Eredivisie'},
    'B1': {'country': 'Belgium', 'league': 'Jupiler League'},
    'P1': {'country': 'Portugal', 'league': 'Liga I'},
    'T1': {'country': 'Turkey', 'league': 'Futbol Ligi 1'},
    'G1': {'country': 'Greece', 'league': 'Ethniki Katigoria'},
}


dict_countries_core_euro = {
    'england': 'https://www.football-data.co.uk/englandm.php',
    'scotland': 'https://www.football-data.co.uk/scotlandm.php',
    'germany': 'https://www.football-data.co.uk/germanym.php',
    'italy': 'https://www.football-data.co.uk/italym.php',
    'spain': 'https://www.football-data.co.uk/spainm.php',
    'france': 'https://www.football-data.co.uk/francem.php',
    'netherlands': 'https://www.football-data.co.uk/netherlandsm.php',
    'belgium': 'https://www.football-data.co.uk/belgiumm.php',
    'portugal': 'https://www.football-data.co.uk/portugalm.php',
    'turkey': 'https://www.football-data.co.uk/turkeym.php',
    'greece': 'https://www.football-data.co.uk/greecem.php',
}

list_extra_leagues_countries = [
    'ARG',
    'AUT',
    'BRA',
    'CHN',
    'DNK',
    'FIN',
    'IRL',
    'JPN',
    'MEX',
    'NOR',
    'POL',
    'ROU',
    'RUS',
    'SWE',
    'SWZ',
    'USA',
]


def main():
    try:
        download_and_save_data_landing_euro()
        # download_and_save_data_landing_euro_new()
    except KeyboardInterrupt:
        _log.info('Process interrupted. Exiting...')


def download_and_save_data_landing_euro():
    list_all_csvs = []
    for country, url in dict_countries_core_euro.items():
        _log.info(f'getting {country}')
        req = requests.get(url)

        regex_pattern = r'<A\s+HREF="([^"]+\.csv)">([^<]+)</A>'
        matches = re.findall(regex_pattern, req.text)
        list_all_csvs.extend([{'country': country, 'season': ele[0].split('/')[1][:2] + '/' + ele[0].split('/')[1][2:], 'league': ele[1], 'link': f'https://www.football-data.co.uk/{ele[0]}'} for ele in matches])

    with ThreadPoolExecutor() as executor:
        executor.map(download_file, list_all_csvs)


def download_and_save_data_landing_euro_new():
    list_all_csvs = []
    for new_league in list_extra_leagues_countries:
        list_all_csvs.append(
            {
                'link': f'https://www.football-data.co.uk/new/{new_league}.csv',
                'country': new_league.lower(),
                'season': 'all',
                'league': new_league.lower(),
            },
        )

    with ThreadPoolExecutor() as executor:
        executor.map(download_file, list_all_csvs)


def download_file(dict_season):
    ulid_thread = str(ulid.ULID())
    link = dict_season['link']
    country = dict_season['country']
    season = dict_season['season'].replace('/', '_')
    league = dict_season['league']
    try:
        output_dir = os.path.join('src', 'data', '01-landing', 'football-co', country, season)
        os.makedirs(output_dir, exist_ok=True)
        filename = os.path.join(output_dir, f'{league}.csv')
        response = requests.head(link)
        if 'Last-Modified' in response.headers:
            last_modified = response.headers['Last-Modified']
        else:
            _log.info(f'[{country:10}] [{season:10}] [{league:30}] - ({ulid_thread}) - No Last-Modified header found for {link}.')
            last_modified = None

        if os.path.exists(filename):
            file_modified_time = os.path.getmtime(filename)
        else:
            file_modified_time = None

        download = False
        if last_modified and file_modified_time:
            server_time = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
            server_time = time.mktime(server_time.timetuple())
            if server_time > file_modified_time:
                download = True
            else:
                _log.info(f'[{country:10}] [{season:10}] [{league:30}] - ({ulid_thread}) - Skipping download. File is unchanged by date headers.')
        else:
            download = True

        if download:
            _log.info(f'[{country:10}] [{season:10}] [{league:30}] - ({ulid_thread}) - Downloading new file from {link}')
            response = requests.get(link)
            header_line = response.text.splitlines()[0].split(',')
            header_line = [remove_non_ascii(col) for col in header_line if col != '']
            df = pd.read_csv(link, encoding='cp1252', sep=',', usecols=[i for i in range(len(header_line))])
            df.columns = header_line
            df.dropna(how='all', inplace=True)
            _log.info(f'[{country:10}] [{season:10}] [{league:30}] - ({ulid_thread}) - Shape {df.shape}')
            _log.info(f'[{country:10}] [{season:10}] [{league:30}] - ({ulid_thread}) - Saving at {filename}')
            df.to_csv(filename, index=False)
            _log.info(f'[{country:10}] [{season:10}] [{league:30}] - ({ulid_thread}) - Saved')

    except Exception as e:
        _log.info(f'[{country:10}] [{season:10}] [{league:30}] - ({ulid_thread}) - Exception: {e}')
        _log.info(f'[{country:10}] [{season:10}] [{league:30}] - Exception: {e}')


def remove_non_ascii(input_string):
    return ''.join(char for char in input_string if ord(char) < 128)


if __name__ == '__main__':
    main()
