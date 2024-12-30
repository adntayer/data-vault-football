from sqlalchemy import Column
from sqlalchemy import Float
from sqlalchemy import String

from src.db.models.base import Base
from src.db.models.base import SCHEMA_SATELLITE


class SatMatchesCoreResults(Base):
    __tablename__ = 'sat_matches_core_results'
    __table_args__ = {'schema': SCHEMA_SATELLITE}

    ulid = Column('ulid', String, primary_key=True, index=True, comment='ULID id for row')
    hk_hub = Column('hk_hub', String, index=True, comment='Hash Key for hub business key')
    fthg = Column('fthg', Float, nullable=False, comment='Full time home goals')
    ftag = Column('ftag', Float, nullable=False, comment='Full time away goals')
    ftr = Column('ftr', Float, nullable=False, comment='Full time result')

    hthg = Column('hthg', Float, comment='Half time home goals')
    htag = Column('htag', Float, comment='Half time away goals')
    htr = Column('htr', Float, comment='Half time result')

    hash_diff = Column('hash_diff', String, index=True, comment='Hash of row')

    ldts = Column('ldts', String, comment='Load timestamp of row')
    src = Column('src', String, comment='Source of data')
    src_record = Column('src_record', String, comment='Record source of data')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
