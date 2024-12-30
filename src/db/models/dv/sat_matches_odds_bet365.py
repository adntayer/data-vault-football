from sqlalchemy import Column
from sqlalchemy import Float
from sqlalchemy import String

from src.db.models.base import Base
from src.db.models.base import SCHEMA_SATELLITE


class SatMatchesOddsBet365(Base):
    __tablename__ = 'sat_matches_odds_bet365'
    __table_args__ = {'schema': SCHEMA_SATELLITE}

    ulid = Column('ulid', String, primary_key=True, index=True, comment='ULID id for row')
    hk_hub = Column('hk_hub', String, index=True, comment='Hash Key for hub business key')

    b365h = Column('b365h', Float, comment='Bet365 home win odds')
    b365d = Column('b365d', Float, comment='Bet365 draw odds')
    b365a = Column('b365a', Float, comment='Bet365 away win odds')
    b365ch = Column('b365ch', Float, comment='Bet365 home win closing odds')
    b365cd = Column('b365cd', Float, comment='Bet365 draw closing odds')
    b365ca = Column('b365ca', Float, comment='Bet365 away win closing odds')
    b365_over_2_5 = Column('b365_over_2_5', Float, comment='Bet365 over 2.5 goals')
    b365_under_2_5 = Column('b365_under_2_5', Float, comment='Bet365 under 2.5 goals')

    hash_diff = Column('hash_diff', String, index=True, comment='Hash of row')
    ldts = Column('ldts', String, comment='Load timestamp of row')
    src = Column('src', String, comment='Source of data')
    src_record = Column('src_record', String, comment='Record source of data')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
