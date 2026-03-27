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
from data_pipeline.database import Base

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
    niveau_activite = Column(String(100))
    type_maladie = Column(String(255))
    severite = Column(String(50))
    restrictions_alimentaires = Column(Text)
    allergies = Column(Text)
    objectif_principal = Column(String(200))
    experience_sportive = Column(String(50))
    frequence_entrainement = Column(Integer)

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

    unite_mesure = Column(String(50), server_default="portion")


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
    hydratation_litres = Column(Numeric(4, 2))

    # Clé étrangère
    id_utilisateur = Column(
        Integer,
        ForeignKey("utilisateur.id_utilisateur", ondelete="CASCADE"),
        nullable=False,
    )

    # Relation
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
