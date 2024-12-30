import pytest

from src.db.models.loader import load_dict_models_raw_data_vault


@pytest.fixture
def dict_models():
    return load_dict_models_raw_data_vault()


def test_db_models_raw_data_vault_number_of_tables_all(dict_models):
    assert len(dict_models.keys()) == 10


def test_db_models_raw_data_vault_number_of_tables_hubs(dict_models):
    assert sum([1 for key in dict_models.keys() if key.startswith('hub_')]) == 4


def test_db_models_raw_data_vault_number_of_tables_links(dict_models):
    assert sum([1 for key in dict_models.keys() if key.startswith('lnk_')]) == 1


def test_db_models_raw_data_vault_number_of_tables_stats(dict_models):
    assert sum([1 for key in dict_models.keys() if key.startswith('sat_')]) == 5


def test_db_models_raw_data_vault_number_of_tables_pit(dict_models):
    assert sum([1 for key in dict_models.keys() if key.startswith('pit_')]) == 0


def test_db_models_raw_data_vault_number_of_tables_bridges(dict_models):
    assert sum([1 for key in dict_models.keys() if key.startswith('bridges_')]) == 0


def test_db_models_raw_data_vault_naming_convention_startswith(dict_models):
    for table_name in dict_models.keys():
        list_check = [
            table_name.startswith('hub_'),
            table_name.startswith('lnk_'),
            table_name.startswith('sat_'),
            table_name.startswith('pit_'),
            table_name.startswith('brd_'),
        ]
        if not any(list_check):
            raise NameError(f"'{table_name=}' is not on naming convetion for this repo")


def test_db_models_raw_data_vault_fixed_columns_all_models(dict_models):
    for table_name, model in dict_models.items():
        model_obj = model()
        list_columns_obj = [col.name for col in model_obj.__table__.columns]
        assert 'ldts' in list_columns_obj, f"'ldts' column not found on {table_name}"
        assert 'src' in list_columns_obj, f"'src' column not found on {table_name}"


def test_db_models_raw_data_vault_fixed_columns_all_hubs(dict_models):
    for table_name, model in dict_models.items():
        if table_name.startswith('hub_'):
            model_obj = model()
            list_columns_obj = [col.name for col in model_obj.__table__.columns]
            assert 'hk' in list_columns_obj, f"'hk' column not found on {table_name}"
            list_remove_default = list(set(list_columns_obj) - {'hk', 'ldts', 'src', 'src_record'})
            for col in list_remove_default:
                assert col.startswith('bk'), f"'{col=}' at '{table_name=}' is not on naming convetion for this repo"


def test_db_models_raw_data_vault_fixed_columns_all_links(dict_models):
    for table_name, model in dict_models.items():
        if table_name.startswith('lnk_'):
            model_obj = model()
            list_columns_obj = [col.name for col in model_obj.__table__.columns]
            assert 'hk' in list_columns_obj, f"'hk' column not found on {table_name}"
            list_remove_default = list(set(list_columns_obj) - {'hk', 'ldts', 'src', 'src_record'})
            for col in list_remove_default:
                assert col.startswith('hk_'), f"'{col=}' at '{table_name=}' is not on naming convetion for this repo"


def test_db_models_raw_data_vault_fixed_columns_all_sats(dict_models):
    for table_name, model in dict_models.items():
        if table_name.startswith('sat_'):
            model_obj = model()
            list_columns_obj = [col.name for col in model_obj.__table__.columns]
            assert 'ulid' in list_columns_obj, f"'ulid' column not found on {table_name}"
            assert 'hk_hub' in list_columns_obj, f"'hk_hub' column not found on {table_name}"
