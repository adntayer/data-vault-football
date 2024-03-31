from sqlalchemy import Column
from sqlalchemy import String

from src.db.models.base import Base
from src.db.models.base import SCHEMA_HUB


class HubMatches(Base):
    __tablename__ = 'hub_matches'
    __table_args__ = {'schema': SCHEMA_HUB}

    hk = Column('hk', String, primary_key=True, index=True, unique=True, comment='Hash Key for hub business key')
    bk_match_country = Column('bk_match_country', String, index=True, comment='Business Key (country)')
    bk_match_league = Column('bk_match_league', String, index=True, comment='Business Key (league)')
    bk_match_season = Column('bk_match_season', String, index=True, comment='Business Key (season)')
    bk_match_home_team = Column('bk_match_home_team', String, index=True, comment='Business Key (home team)')
    bk_match_away_team = Column('bk_match_away_team', String, index=True, comment='Business Key (away team)')
    bk_match_date = Column('bk_match_date', String, index=True, comment='Business Key (match date)')
    bk_match_time = Column('bk_match_time', String, index=True, comment='Business Key (match time)')

    ldts = Column('ldts', String, comment='Load timestamp of row')
    src = Column('src', String, comment='Source of data')
    src_record = Column('src_record', String, comment='Record source of data')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
