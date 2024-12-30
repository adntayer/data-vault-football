from sqlalchemy import Column
from sqlalchemy import Float
from sqlalchemy import String

from src.db.models.base import Base
from src.db.models.base import SCHEMA_SATELLITE


class SatMatchesOddsMarketConsensus(Base):
    __tablename__ = 'sat_matches_odds_market_consensus'
    __table_args__ = {'schema': SCHEMA_SATELLITE}

    ulid = Column('ulid', String, primary_key=True, index=True, comment='ULID id for row')
    hk_hub = Column('hk_hub', String, index=True, comment='Hash Key for hub business key')

    maxh = Column('maxh', Float, comment='Market maximum home win odds')
    maxch = Column('maxch', Float, comment='Market maximum home win closing odds')
    maxd = Column('maxd', Float, comment='Market maximum draw win odds')
    maxcd = Column('maxcd', Float, comment='Market maximum draw win closing odds')
    maxa = Column('maxa', Float, comment='Market maximum away win odds')
    maxca = Column('maxca', Float, comment='Market maximum away win closing odds')
    avgh = Column('avgh', Float, comment='Market average home win odds')
    avgch = Column('avgch', Float, comment='Market average home win closing odds')
    avgd = Column('avgd', Float, comment='Market average draw odds')
    avgcd = Column('avgcd', Float, comment='Market average draw closing odds')
    avga = Column('avga', Float, comment='Market average away win odds')
    avgca = Column('avgca', Float, comment='Market average away win closing odds')

    max_over_2_5 = Column('max_over_2_5', Float, comment='Market maximum over 2.5 goals')
    max_c_over_2_5 = Column('max_c_over_2_5', Float, comment='Market maximum over 2.5 goals - closing odds')
    max_under_2_5 = Column('max_under_2_5', Float, comment='Market maximum under 2.5 goals')
    max_c_under_2_5 = Column('max_c_under_2_5', Float, comment='Market maximum under 2.5 goals - closing odds')
    avg_over_2_5 = Column('avg_over_2_5', Float, comment='Market average over 2.5 goals')
    avg_c_over_2_5 = Column('avg_c_over_2_5', Float, comment='Market average over 2.5 goals - closing odds')
    avg_under_2_5 = Column('avg_under_2_5', Float, comment='Market average under 2.5 goals')
    avg_c_under_2_5 = Column('avg_c_under_2_5', Float, comment='Market average under 2.5 goals - closing odds')

    hash_diff = Column('hash_diff', String, index=True, comment='Hash of row')
    ldts = Column('ldts', String, comment='Load timestamp of row')
    src = Column('src', String, comment='Source of data')
    src_record = Column('src_record', String, comment='Record source of data')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
