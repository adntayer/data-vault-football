from sqlalchemy import Column
from sqlalchemy import String

from src.db.models.base import Base
from src.db.models.base import SCHEMA_HUB


class HubSeasons(Base):
    __tablename__ = 'hub_seasons'
    __table_args__ = {'schema': SCHEMA_HUB}

    hk = Column('hk', String, primary_key=True, index=True, unique=True, comment='Hash Key for hub business key')
    bk = Column('bk', String, index=True, comment='Business Key (season)')

    ldts = Column('ldts', String, comment='Load timestamp of row')
    src = Column('src', String, comment='Source of data')
    src_record = Column('src_record', String, comment='Record source of data')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
