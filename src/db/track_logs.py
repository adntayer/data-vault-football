import datetime
import time
from contextlib import contextmanager

import psutil
import ulid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.models import MonitorLogs
from src.settings import DB_URI_OLTP


@contextmanager
def get_session():
    engine = create_engine(DB_URI_OLTP)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def log_start(ulid_thread, source, process):
    with get_session() as session:
        log_entry = MonitorLogs(
            unique_id=str(ulid.ULID()),
            ulid_thread=ulid_thread,
            source=source,
            process=process,
            status='started',
            started_at=datetime.datetime.now(),
            execution_time=0,
            memory_usage=0,
            cpu_time=0,
        )
        session.add(log_entry)


def log_success(ulid_thread, start_time, start_memory, start_cpu_time):
    end_memory = psutil.Process().memory_info().rss
    execution_time = time.time() - start_time
    cpu_time_used = time.process_time() - start_cpu_time
    cpu_time_ratio = cpu_time_used / execution_time if execution_time > 0 else 0

    with get_session() as session:
        log_entry = (
            session.query(MonitorLogs)
            .filter(
                MonitorLogs.ulid_thread == ulid_thread,
                MonitorLogs.status == 'started',
            )
            .first()
        )

        if log_entry:
            log_entry.status = 'success'
            log_entry.finished_at = datetime.datetime.now()
            duration = log_entry.finished_at - log_entry.started_at
            log_entry.duration = duration.total_seconds()
            log_entry.execution_time = round(execution_time, 2)
            log_entry.memory_usage = round((end_memory - start_memory) / (1024 * 1024), 2)
            log_entry.cpu_time = round(cpu_time_used, 2)
            log_entry.cpu_time_ratio = round(cpu_time_ratio, 2)
            session.commit()


def log_error(ulid_thread, error_message, start_time, start_memory, start_cpu_time):
    end_memory = psutil.Process().memory_info().rss
    execution_time = time.time() - start_time
    cpu_time_used = time.process_time() - start_cpu_time
    cpu_time_ratio = cpu_time_used / execution_time if execution_time > 0 else 0

    with get_session() as session:
        log_entry = (
            session.query(MonitorLogs)
            .filter(
                MonitorLogs.ulid_thread == ulid_thread,
                MonitorLogs.status == 'started',
            )
            .first()
        )

        if log_entry:
            log_entry.status = 'error'
            log_entry.finished_at = datetime.datetime.now()
            log_entry.error_message = error_message

            if log_entry.started_at:
                duration = log_entry.finished_at - log_entry.started_at
                log_entry.duration = duration.total_seconds()
                log_entry.execution_time = round(execution_time, 2)
                log_entry.memory_usage = round((end_memory - start_memory) / (1024 * 1024), 2)
                log_entry.cpu_time = round(cpu_time_used, 2)
                log_entry.cpu_time_ratio = round(cpu_time_ratio, 2)
            session.commit()
