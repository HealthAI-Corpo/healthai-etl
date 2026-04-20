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
    Enum,
)
from sqlalchemy.orm import relationship
from src.data_pipeline.database import Base
from enum import Enum as PyEnum

# --- TABLES RÉFÉRENTIELS  ---


class Utilisateur(Base):
    __tablename__ = "utilisateur"

    id_utilisateur = Column(Integer, primary_key=True, index=True)
    nom = Column(String(50), nullable=False)
    prenom = Column(String(50), nullable=False)
    email = Column(String(255), unique=True, nullable=False, index=True)
    date_de_naissance = Column(Date, nullable=False)
    genre = Column(String(50), nullable=False)
    mot_de_passe_hash = Column(String(255), nullable=False)
    type_abonnement = Column(String(50), server_default="Freemium")
    date_inscription = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    profil_sante = relationship(
        "ProfilSante",
        back_populates="utilisateur",
        uselist=False,
        cascade="all, delete-orphan",
    )

    # Relation logs
    alimentation_logs = relationship(
        "LogAliment", back_populates="utilisateur", cascade="all, delete-orphan"
    )
    seance_logs = relationship(
        "LogSeance", back_populates="utilisateur", cascade="all, delete-orphan"
    )
    sante_logs = relationship(
        "LogSante", back_populates="utilisateur", cascade="all, delete-orphan"
    )


class ProfilSante(Base):
    __tablename__ = "profil_sante"

    id_profil = Column(Integer, primary_key=True, index=True)
    id_utilisateur = Column(ForeignKey("utilisateur.id_utilisateur"), unique=True)
    poids_kg = Column(Numeric(5, 2), nullable=False)
    taille_cm = Column(Integer, nullable=False)
    imc = Column(Numeric(4, 1))
    type_maladie = Column(String(255))
    severite = Column(String(50))
    restrictions_alimentaires = Column(Text)
    allergies = Column(Text)
    objectif_principal = Column(String(200), nullable=True)
    experience_sportive = Column(String(100))
    heures_entrainement_semaine = Column(Numeric(4, 1))

    # Relation
    utilisateur = relationship("Utilisateur", back_populates="profil_sante")


class Aliment(Base):
    __tablename__ = "aliment"

    id_aliment = Column(Integer, primary_key=True, index=True)
    nom = Column(String(255), nullable=False)
    categorie = Column(String(100))
    type_repas = Column(String(50))

    calories = Column(Numeric(6, 1), nullable=False)
    proteines = Column(Numeric(5, 2), nullable=False)
    lipides = Column(Numeric(5, 2), nullable=False)
    glucides = Column(Numeric(5, 2), nullable=False)

    fibres = Column(Numeric(5, 2))
    sucres = Column(Numeric(5, 2))
    sodium_mg = Column(Numeric(7, 2))
    cholesterol_mg = Column(Numeric(7, 2))
    eau_ml = Column(Numeric(7, 2))

    unite_mesure = Column(String(50), server_default="portion")


class Exercice(Base):
    __tablename__ = "exercice"

    id_exercice = Column(Integer, primary_key=True, index=True)
    nom = Column(String(150), nullable=False)
    type_exercice = Column(String(100), nullable=False)
    muscles_principaux = Column(String(100))
    muscles_secondaires = Column(String(100))
    equipement = Column(String(100))
    difficulte = Column(String(50))
    instructions = Column(Text)


# --- TABLES DATASET ---


class DatasetRecommendationsRegime(Base):
    __tablename__ = "dataset_recommendations_regime"

    id_dataset_recommendations_regime = Column(Integer, primary_key=True, index=True)
    age = Column(Integer)
    sexe = Column(String(50))
    poids_kg = Column(Numeric(5, 2))
    taille_cm = Column(Integer)
    type_maladie = Column(String(255))
    gravite = Column(String(50))
    niveau_activite_physique = Column(String(100))
    apport_calorique_journalier = Column(Integer)
    cholesterol_mg_dl = Column(Numeric(6, 2))
    tension_arterielle_mmhg = Column(Numeric(6, 2))
    glucose_mg_dl = Column(Numeric(6, 2))
    restrictions_alimentaires = Column(String(255))
    allergies = Column(String(255))
    cuisine_preferee = Column(String(100))
    heures_exercice_semaine = Column(Numeric(4, 2))
    adherence_regime = Column(Numeric(5, 2))
    score_desiquilibre_nutriment = Column(Numeric(4, 1))
    recommendation_regime = Column(String(255))


class DatasetHistoriqueSeanceExercice(Base):
    __tablename__ = "dataset_historique_seance_exercice"

    id_dataset_historique_seance_exercice = Column(
        Integer, primary_key=True, index=True
    )
    age = Column(Integer)
    sexe = Column(String(50))
    poids_kg = Column(Numeric(5, 2))
    taille_cm = Column(Integer)
    bpm_max = Column(Integer)
    bpm_moyen = Column(Integer)
    bpm_repos = Column(Integer)
    duree_seance_minutes = Column(Numeric(5, 1))
    calories_brulees = Column(Numeric(6, 1))
    type_sport = Column(String(100))
    pourcentage_gras = Column(Numeric(5, 1))
    consommation_eau_ml = Column(Numeric(8, 2))
    frequence_sport_jour_semaine = Column(Integer)
    niveau_experience = Column(Integer)


# --- TABLES DE LOGS ---


class LogAliment(Base):
    __tablename__ = "log_aliment"

    id_log_aliment = Column(Integer, primary_key=True, index=True)
    log_date = Column(TIMESTAMP, nullable=False)
    repas = Column(String(50), nullable=False)
    quantite = Column(Numeric(7, 2), nullable=False)
    unite = Column(String(20), server_default="g")  # g ou ml

    # Clés étrangères
    id_aliment = Column(Integer, ForeignKey("aliment.id_aliment"), nullable=False)
    id_utilisateur = Column(
        Integer,
        ForeignKey("utilisateur.id_utilisateur", ondelete="CASCADE"),
        nullable=False,
    )

    # Relations
    utilisateur = relationship("Utilisateur", back_populates="alimentation_logs")
    aliment = relationship("Aliment")


class LogSeance(Base):
    __tablename__ = "log_seance"

    id_seance_log = Column(Integer, primary_key=True, index=True)
    log_date = Column(TIMESTAMP, nullable=False)
    type_seance = Column(String(50))
    duree_minutes = Column(Numeric(5, 1), nullable=False)
    calorie_brulee = Column(Numeric(6, 1), nullable=False)
    bpm_moyen = Column(Integer)

    # Clés étrangères
    # On met nullable=True pour id_exercice car une séance peut être un "global"
    # sans pointer vers un exercice spécifique de l'API
    id_exercice = Column(Integer, ForeignKey("exercice.id_exercice"), nullable=True)
    id_utilisateur = Column(
        Integer,
        ForeignKey("utilisateur.id_utilisateur", ondelete="CASCADE"),
        nullable=False,
    )

    # Relations
    utilisateur = relationship("Utilisateur", back_populates="seance_logs")
    exercice = relationship("Exercice")


class LogSante(Base):
    __tablename__ = "log_sante"

    id_log_sante = Column(Integer, primary_key=True, index=True)
    date_log = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    poids_kg = Column(Numeric(5, 2), nullable=False)
    pourcentage_gras = Column(Numeric(4, 1))
    imc_actuel = Column(Numeric(4, 1))
    bpm_repos = Column(Integer)
    bpm_moyen_journee = Column(Integer)
    heures_sommeil = Column(Numeric(4, 2))
    nb_pas = Column(Integer, default=0)
    hydratation_ml = Column(Numeric(7, 2))

    # Clé étrangère
    id_utilisateur = Column(
        Integer,
        ForeignKey("utilisateur.id_utilisateur", ondelete="CASCADE"),
        nullable=False,
    )

    # Relation
    utilisateur = relationship("Utilisateur", back_populates="sante_logs")


# --- TABLES etl_log ---


class StatutEtlEnum(str, PyEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    FAILURE = "FAILURE"


class EtlLog(Base):
    __tablename__ = "etl_log"

    id_etl_log = Column(Integer, primary_key=True, index=True)
    libelle_pipeline = Column(String(255), nullable=False)
    fichier_nom = Column(String(255), nullable=False)
    date_execution = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    nb_lignes_total = Column(Integer)
    nb_lignes_valides = Column(Integer)
    nb_lignes_anomalies = Column(Integer)
    statut = Column(Enum(StatutEtlEnum), nullable=True)
    message = Column(Text, nullable=True)
