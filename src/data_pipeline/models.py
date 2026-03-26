from sqlalchemy import (
    Column,
    Integer,
    String,
    Numeric,
    Date,
    TIMESTAMP,
    text,
    ForeignKey,
    Text,
    Boolean,
)
from sqlalchemy.orm import relationship
from src.data_pipeline.database import Base

# --- TABLES RÉFÉRENTIELS  ---


class Utilisateur(Base):
    __tablename__ = "utilisateur"

    id_utilisateur = Column(Integer, primary_key=True, index=True)
    nom = Column(String(50), nullable=False)
    prenom = Column(String(50), nullable=False)
    email = Column(String(255), unique=True, nullable=False, index=True)
    date_de_naissance = Column(Date, nullable=False)
    genre = Column(String(50), nullable=False)
    objectif_principal = Column(String(200), nullable=False)
    poids_actuel = Column(Numeric(5, 2), nullable=False)
    taille_cm = Column(Integer, nullable=False)
    type_abonnement = Column(String(50), server_default="Freemium")
    date_inscription = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    mot_de_passe_hash = Column(String(255), nullable=False)

    # Relations
    alimentation_logs = relationship(
        "LogAliment", back_populates="utilisateur", cascade="all, delete-orphan"
    )
    seance_logs = relationship(
        "LogSeance", back_populates="utilisateur", cascade="all, delete-orphan"
    )
    sante_logs = relationship(
        "LogSante", back_populates="utilisateur", cascade="all, delete-orphan"
    )


class Aliment(Base):
    __tablename__ = "aliment"

    id_aliment = Column(Integer, primary_key=True, index=True)
    nom = Column(String(100), nullable=False)
    calories = Column(Numeric(6, 1), nullable=False)
    proteines = Column(Numeric(4, 1), nullable=False)
    lipides = Column(Numeric(4, 1), nullable=False)
    glucides = Column(Numeric(4, 1), nullable=False)
    unite_mesure = Column(String(20), server_default="100g")


class Exercice(Base):
    __tablename__ = "exercice"

    id_exercice = Column(Integer, primary_key=True, index=True)
    nom = Column(String(150), nullable=False)
    type_exercice = Column(String(100), nullable=False)
    muscle_cible = Column(String(100))
    equipement = Column(String(100))
    difficulte = Column(String(50))
    instructions = Column(Text)


# --- TABLES DE LOGS (Historiques) ---


class LogAliment(Base):
    __tablename__ = "log_aliment"

    id_log_aliment = Column(Integer, primary_key=True, index=True)
    log_date = Column(TIMESTAMP, nullable=False)
    repas = Column(String(50), nullable=False)
    quantite_g = Column(Numeric(7, 2), nullable=False)

    id_aliment = Column(Integer, ForeignKey("aliment.id_aliment"), nullable=False)
    id_utilisateur = Column(
        Integer,
        ForeignKey("utilisateur.id_utilisateur", ondelete="CASCADE"),
        nullable=False,
    )

    utilisateur = relationship("Utilisateur", back_populates="alimentation_logs")


class LogSeance(Base):
    __tablename__ = "log_seance"

    id_seance_log = Column(Integer, primary_key=True, index=True)
    log_date = Column(TIMESTAMP, nullable=False)
    duree_exercice = Column(Numeric(5, 1), nullable=False)
    calorie_brulee = Column(Numeric(6, 1), nullable=False)

    id_exercice = Column(Integer, ForeignKey("exercice.id_exercice"), nullable=False)
    id_utilisateur = Column(
        Integer,
        ForeignKey("utilisateur.id_utilisateur", ondelete="CASCADE"),
        nullable=False,
    )

    utilisateur = relationship("Utilisateur", back_populates="seance_logs")


class LogSante(Base):
    __tablename__ = "log_sante"

    id_log_sante = Column(Integer, primary_key=True, index=True)
    date_log = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    poids_kg = Column(Numeric(5, 2), nullable=False)
    moyenne_bpm = Column(Numeric(4, 1), nullable=False)
    heures_sommeil = Column(Numeric(4, 2), nullable=False)
    nb_pas = Column(Integer, default=0)
    frequence_cardiaque = Column(Integer)

    id_utilisateur = Column(
        Integer,
        ForeignKey("utilisateur.id_utilisateur", ondelete="CASCADE"),
        nullable=False,
    )

    utilisateur = relationship("Utilisateur", back_populates="sante_logs")


# --- TABLES ANOMALIES D'IMPORT ---


class UtilisateurImportAnomalies(Base):
    __tablename__ = "utilisateur_import_anomalies"

    id = Column(Integer, primary_key=True, index=True)
    nom = Column(String(1000), nullable=True)
    prenom = Column(String(1000), nullable=True)
    email = Column(String(1000), nullable=True)
    date_de_naissance = Column(String(1000), nullable=True)
    genre = Column(String(1000), nullable=True)
    objectif_principal = Column(String(1000), nullable=True)
    poids_actuel = Column(String(1000), nullable=True)
    taille_cm = Column(String(1000), nullable=True)
    type_abonnement = Column(String(1000), nullable=True)
    date_inscription = Column(String(1000), nullable=True)
    mot_de_passe_hash = Column(String(1000), nullable=True)
    erreur = Column(Text, nullable=False)
    est_corrige = Column(Boolean, nullable=False, server_default=text("false"))
    date_import = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))


class AlimentImportAnomalies(Base):
    __tablename__ = "aliment_import_anomalies"

    id = Column(Integer, primary_key=True, index=True)
    nom = Column(String(1000), nullable=True)
    calories = Column(String(1000), nullable=True)
    proteines = Column(String(1000), nullable=True)
    lipides = Column(String(1000), nullable=True)
    glucides = Column(String(1000), nullable=True)
    unite_mesure = Column(String(1000), nullable=True)
    erreur = Column(Text, nullable=False)
    est_corrige = Column(Boolean, nullable=False, server_default=text("false"))
    date_import = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))


class ExerciceImportAnomalies(Base):
    __tablename__ = "exercice_import_anomalies"

    id = Column(Integer, primary_key=True, index=True)
    nom = Column(String(1000), nullable=True)
    type_exercice = Column(String(1000), nullable=True)
    muscle_cible = Column(String(1000), nullable=True)
    equipement = Column(String(1000), nullable=True)
    difficulte = Column(String(1000), nullable=True)
    instructions = Column(String(1000), nullable=True)
    erreur = Column(Text, nullable=False)
    est_corrige = Column(Boolean, nullable=False, server_default=text("false"))
    date_import = Column(TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
