from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base


SCHEMA_HUB = 'main'
SCHEMA_LINK = 'main'
SCHEMA_SATELLITE = 'main'
SCHEMA_PIT = 'main'
SCHEMA_BRIDGE = 'main'
SCHEMA_BACKSTAGE = 'main'
SCHEMA_MAIN = 'main'

list_schemas_raw_data_vault = [SCHEMA_HUB, SCHEMA_LINK, SCHEMA_SATELLITE, SCHEMA_PIT, SCHEMA_BRIDGE]

# https://alembic.sqlalchemy.org/en/latest/naming.html
meta = MetaData(
    naming_convention={
        'ix': 'ix_%(column_0_label)s',
        'uq': 'uq_%(table_name)s_%(column_0_N_name)s',
        'ck': 'ck_%(table_name)s_%(constraint_name)s',
        'fk': 'fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s',
        'pk': 'pk_%(table_name)s',
    },
)

Base = declarative_base(metadata=meta)
