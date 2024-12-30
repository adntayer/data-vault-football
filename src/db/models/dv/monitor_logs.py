from sqlalchemy import Column
from sqlalchemy import Float
from sqlalchemy import String
from sqlalchemy import TIMESTAMP

from src.db.models.base import Base
from src.db.models.base import SCHEMA_BACKSTAGE


class MonitorLogs(Base):
    __tablename__ = 'monitor_logs'
    __table_args__ = {'schema': SCHEMA_BACKSTAGE}

    unique_id = Column(String, primary_key=True)
    ulid_thread = Column(String(26), index=True, comment='ULID thread identifier')
    source = Column(String(50), index=True, comment='Name of the function logging the process')
    process = Column(String(50), index=True, comment='Name of the process that is running')
    status = Column(String(30), comment='Process status: started, finished, or error')
    started_at = Column(TIMESTAMP, comment='Timestamp when the process started')
    finished_at = Column(TIMESTAMP, comment='Timestamp when the process finished')
    duration = Column(Float, comment='Duration between start and finish')
    memory_usage = Column(Float, comment='Memory used in MB')
    execution_time = Column(Float, comment='Execution time in seconds')
    cpu_time = Column(Float, comment='CPU time used in seconds')
    cpu_time_ratio = Column(Float, comment='Calculate the ratio cpu_time_used / execution_time')

    error_message = Column(String, comment='Error message if any error occurred')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
