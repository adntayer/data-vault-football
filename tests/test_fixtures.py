import pandas as pd

from src.data_vault.football_co.extractor import dict_countries_core_euro
from src.data_vault.football_co.extractor import list_extra_leagues_countries


def test_dict_countries_core_euro():
    dict_countries_core_euro_desired = {
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

    assert sorted(dict_countries_core_euro.keys()) == sorted(dict_countries_core_euro_desired.keys())
    for k, v in dict_countries_core_euro.items():
        assert v == dict_countries_core_euro_desired[k]


def test_list_extra_leagues_countries_fixed():
    list_extra_leagues_countries_desired = ['ARG', 'AUT', 'BRA', 'CHN', 'DNK', 'FIN', 'IRL', 'JPN', 'MEX', 'NOR', 'POL', 'ROU', 'RUS', 'SWE', 'SWZ', 'USA']

    assert sorted(list_extra_leagues_countries) == sorted(list_extra_leagues_countries_desired)


def test_list_extra_leagues_countries_real_time():
    list_extra_leagues_countries_desired = ['ARG', 'AUT', 'BRA', 'CHN', 'DNK', 'FIN', 'IRL', 'JPN', 'MEX', 'NOR', 'POL', 'ROU', 'RUS', 'SWE', 'SWZ', 'USA']

    url_base = 'https://www.football-data.co.uk/new/new_leagues_data.xlsx'
    xl = pd.ExcelFile(url_base)
    countries = xl.sheet_names

    assert sorted(countries) == sorted(list_extra_leagues_countries_desired)
