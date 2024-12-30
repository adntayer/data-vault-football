from sqlalchemy import Column
from sqlalchemy import String

from src.db.models.base import Base
from src.db.models.base import SCHEMA_LINK


class LnkMatches(Base):
    __tablename__ = 'lnk_matches'
    __table_args__ = {'schema': SCHEMA_LINK}

    hk = Column('hk', String, primary_key=True, index=True, comment='Hash Key for hub business key')
    hk_match_home_team = Column('hk_match_home_team', String, index=True, nullable=False, comment='Hash Key for hk_match_home_team')
    hk_match_away_team = Column('hk_match_away_team', String, index=True, nullable=False, comment='Hash Key for hk_match_away_team')
    hk_match_league = Column('hk_match_league', String, index=True, nullable=False, comment='Hash Key for hk_match_league')
    hk_match_season = Column('hk_match_season', String, index=True, nullable=False, comment='Hash Key for hk_match_season')
    hk_match_country = Column('hk_match_country', String, index=True, nullable=False, comment='Hash Key for hk_match_country')

    ldts = Column('ldts', String, comment='Load timestamp of row')
    src = Column('src', String, comment='Source of data')
    src_record = Column('src_record', String, comment='Record source of data')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
