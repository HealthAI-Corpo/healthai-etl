"""Add EtlLog table with StatutEtlEnum

Revision ID: 8a1c2d3e4f5a
Revises: 4279a143b14f
Create Date: 2026-04-10 14:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '8a1c2d3e4f5a'
down_revision: Union[str, Sequence[str], None] = '4279a143b14f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - add etl_log table."""
    # SQLAlchemy will create the ENUM type automatically when creating the table
    op.create_table(
        'etl_log',
        sa.Column('id_etl_log', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('libelle_pipeline', sa.String(length=255), nullable=False),
        sa.Column('fichier_nom', sa.String(length=255), nullable=False),
        sa.Column('date_execution', postgresql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.Column('nb_lignes_total', sa.Integer(), nullable=True),
        sa.Column('nb_lignes_valides', sa.Integer(), nullable=True),
        sa.Column('nb_lignes_anomalies', sa.Integer(), nullable=True),
        sa.Column('statut', postgresql.ENUM('PENDING', 'SUCCESS', 'PARTIAL_FAILURE', 'FAILURE', name='statutetlenum'), nullable=True),
        sa.Column('message', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id_etl_log', name=op.f('etl_log_pkey'))
    )
    op.create_index(op.f('ix_etl_log_id_etl_log'), 'etl_log', ['id_etl_log'], unique=False)


def downgrade() -> None:
    """Downgrade schema - remove etl_log table."""
    op.drop_index(op.f('ix_etl_log_id_etl_log'), table_name='etl_log')
    op.drop_table('etl_log')
    op.execute("DROP TYPE IF EXISTS statutetlenum")
