# python -m src.db.models.upgrade_db
import subprocess

import alembic.config

from src.logger import SetupLogger

_log = SetupLogger('src.db.models.upgrade_db')


def get_git_commit_message():
    commit_message = (
        subprocess.check_output(
            ['git', 'log', '-1', '--pretty=%B'],
            stderr=subprocess.STDOUT,
        )
        .strip()
        .decode('utf-8')
    )
    return commit_message


def migrate():
    commit_message = get_git_commit_message()
    if commit_message:
        alembicArgs = [
            '--raiseerr',
            'revision',
            '--autogenerate',
            '-m',
            commit_message,
        ]
        alembic.config.main(argv=alembicArgs)


def upgrade_head():
    alembicArgs = [
        '--raiseerr',
        'upgrade',
        'head',
    ]
    alembic.config.main(argv=alembicArgs)


if __name__ == '__main__':
    try:
        migrate()
    except Exception as e:
        print(f'cant migrate: {e}')
    upgrade_head()
