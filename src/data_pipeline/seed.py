from decimal import Decimal
from src.data_pipeline.database import SessionLocal
from src.data_pipeline.models import Utilisateur, ProfilSante
from src.utils.logger import logger


def seed_test_data():
    """Génère des données de test cohérentes pour le développement du Front"""
    db = SessionLocal()
    if not db:
        logger.error("Erreur : La session de base de données n'est pas initialisée.")
        return

    try:
        # Nettoyage des anciennes données de test pour repartir à zéro
        # ProfilSante dépend d'Utilisateur
        db.query(ProfilSante).delete()
        db.query(Utilisateur).delete()
        logger.info("Anciennes données de test supprimées")

        # ---  CRÉATION DES UTILISATEURS ---
        users = [
            Utilisateur(id_utilisateur=1, nom="Jordan", email="jordan@test.com"),
            Utilisateur(id_utilisateur=2, nom="Alice", email="alice@health.ai"),
            Utilisateur(id_utilisateur=3, nom="Marc", email="marc@fitness.fr"),
        ]
        db.add_all(users)
        db.flush()  # Envoie les utilisateurs en base pour que les Profils puissent s'y lier

        # ---  CRÉATION DES PROFILS SANTÉ ASSOCIÉS ---
        profils = [
            # Profil 1 : Sportif sans soucis particuliers
            ProfilSante(
                id_utilisateur=1,
                poids_kg=Decimal("78.50"),
                taille_cm=182,
                experience_sportive="ACTIVE",
                type_maladie=None,
                severite="None",
                restrictions_alimentaires=None,
                allergies="Peanuts",
                heures_entrainement_semaine=6.0,
                objectif_principal="Prise de muscle et endurance",
            ),
            # Profil 2 : Sédentaire avec pathologie (Diabète)
            ProfilSante(
                id_utilisateur=2,
                poids_kg=Decimal("92.00"),
                taille_cm=160,
                experience_sportive="SEDENTARY",
                type_maladie="Diabetes",
                severite="Moderate",
                restrictions_alimentaires="Low Sugar",
                allergies=None,
                heures_entrainement_semaine=1.5,
                objectif_principal="Perte de poids contrôlée",
            ),
            # Profil 3 : Sportif avec Hypertension
            ProfilSante(
                id_utilisateur=3,
                poids_kg=Decimal("70.00"),
                taille_cm=175,
                experience_sportive="ACTIVE",
                type_maladie="Hypertension",
                severite="Mild",
                restrictions_alimentaires="Low Sodium",
                allergies="Gluten",
                heures_entrainement_semaine=8.0,
                objectif_principal="Maintenir la forme malgré la tension",
            ),
        ]

        db.add_all(profils)
        db.commit()
        logger.info(
            "Seeding en base de données réussi | Utilisateurs/Profils créés : {}",
            len(users),
        )

    except Exception as e:
        db.rollback()
        logger.error(
            "Erreur lors du seeding dans la base de données | Erreur : {}", str(e)
        )
    finally:
        db.close()
