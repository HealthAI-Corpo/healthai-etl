"""Drop anomalies tables

Revision ID: 9b2d3e4f5a6b
Revises: 8a1c2d3e4f5a
Create Date: 2026-04-10 14:35:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '9b2d3e4f5a6b'
down_revision: Union[str, Sequence[str], None] = '8a1c2d3e4f5a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema - drop anomalies tables."""
    # Drop anomalies tables with CASCADE to handle dependencies
    tables_to_drop = [
        'aliment_import_anomalies',
        'exercice_import_anomalies',
        'dataset_historique_seance_exercice_import_anomalies',
        'dataset_recommendations_regime_import_anomalies',
        'utilisateur_import_anomalies',
        'profil_sante_import_anomalies'
    ]
    
    for table in tables_to_drop:
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")


def downgrade() -> None:
    """Downgrade schema - note: anomalies tables are not recreated as they were only used for import tracking."""
    pass
