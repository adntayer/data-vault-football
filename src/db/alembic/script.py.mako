"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = ${repr(up_revision)}
down_revision: str = ${repr(down_revision)}

def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
