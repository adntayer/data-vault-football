from sqlalchemy import Column
from sqlalchemy import Float
from sqlalchemy import String

from src.db.models.base import Base
from src.db.models.base import SCHEMA_SATELLITE


class SatMatchesCoreStatistics(Base):
    __tablename__ = 'sat_matches_core_statistics'
    __table_args__ = {'schema': SCHEMA_SATELLITE}

    ulid = Column('ulid', String, primary_key=True, index=True, comment='ULID id for row')
    hk_hub = Column('hk_hub', String, index=True, comment='Hash Key for hub business key')

    attendance = Column('attendance', Float, comment='crowd attendance')
    referee = Column('referee', String, comment='match referee')
    hs = Column('hs', Float, comment='home team shots')
    _as = Column('as', Float, comment='away team shots')
    hst = Column('hst', Float, comment='home team shots on target')
    ast = Column('ast', Float, comment='away team shots on target')
    hhw = Column('hhw', Float, comment='home team hit woodwork')
    ahw = Column('ahw', Float, comment='away team hit woodwork')
    hc = Column('hc', Float, comment='home team corners')
    ac = Column('ac', Float, comment='away team corners')
    hf = Column('hf', Float, comment='home team fouls committed')
    af = Column('af', Float, comment='away team fouls committed')
    hfkc = Column('hfkc', Float, comment='home team free kicks conceded')
    afkc = Column('afkc', Float, comment='away team free kicks conceded')
    ho = Column('ho', Float, comment='home team offsides')
    ao = Column('ao', Float, comment='away team offsides')
    hy = Column('hy', Float, comment='home team yellow cards')
    ay = Column('ay', Float, comment='away team yellow cards')
    hr = Column('hr', Float, comment='home team red cards')
    ar = Column('ar', Float, comment='away team red cards')
    hbp = Column('hbp', Float, comment='home team bookings points (10 = yellow, 25 = red)')
    abp = Column('abp', Float, comment='away team bookings points (10 = yellow, 25 = red)')

    hash_diff = Column('hash_diff', String, index=True, comment='Hash of row')
    ldts = Column('ldts', String, comment='load timestamp of row')
    src = Column('src', String, comment='source of data')
    src_record = Column('src_record', String, comment='Record source of data')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
