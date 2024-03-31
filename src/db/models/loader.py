# python -m src.db.models.loader
import inspect

from src.db import models
from src.logger import SetupLogger


_log = SetupLogger('src.db.models.loader')


def load_dict_models_raw_data_vault():
    dict_models = {}
    _log.info('loading models')

    for name, obj in inspect.getmembers(models):
        if inspect.isclass(obj) and hasattr(obj, '__tablename__'):
            list_allowed_raw_data_vault_by_class_name = [
                'hub' in name.lower(),
                'lnk' in name.lower(),
                'sat' in name.lower(),
                'pit' in name.lower(),
                'brd' in name.lower(),
            ]
            if any(list_allowed_raw_data_vault_by_class_name):
                table_name = obj.__tablename__
                _log.info(f'grabbing {name} ({table_name=})')
                dict_models[table_name] = obj
    return dict_models


if __name__ == '__main__':
    print(load_dict_models_raw_data_vault())
