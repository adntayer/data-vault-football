from sqlalchemy import Column
from sqlalchemy import Float
from sqlalchemy import String

from src.db.models.base import Base
from src.db.models.base import SCHEMA_SATELLITE


class SatMatchesOddsSportingbet(Base):
    __tablename__ = 'sat_matches_odds_sportingbet'
    __table_args__ = {'schema': SCHEMA_SATELLITE}

    ulid = Column('ulid', String, primary_key=True, index=True, comment='ULID id for row')
    hk_hub = Column('hk_hub', String, index=True, comment='Hash Key for hub business key')

    sbh = Column('sbh', Float, comment='Sportingbet home win odds')
    sbd = Column('sbd', Float, comment='Sportingbet draw odds')
    sba = Column('sba', Float, comment='Sportingbet away win odds')
    sbch = Column('sbch', Float, comment='Sportingbet home win closing odds')
    sbcd = Column('sbcd', Float, comment='Sportingbet draw closing odds')
    sbac = Column('sbac', Float, comment='Sportingbet away win closing odds')

    hash_diff = Column('hash_diff', String, index=True, comment='Hash of row')
    ldts = Column('ldts', String, comment='Load timestamp of row')
    src = Column('src', String, comment='Source of data')
    src_record = Column('src_record', String, comment='Record source of data')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
