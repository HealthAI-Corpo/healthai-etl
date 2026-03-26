from src.data_pipeline.database import SessionLocal
from src.data_pipeline.models import Utilisateur
from datetime import date


def run_test_insert():

    session = SessionLocal()

    try:
        print("Insertion de données de test...")

        # Vérification si l'utilisateur existe
        existing_user = (
            session.query(Utilisateur)
            .filter_by(email="jordan.test@healthai.com")
            .first()
        )

        if not existing_user:
            new_user = Utilisateur(
                nom="Jordan",
                prenom="Nkunga",
                email="jordan.test@healthai.com",
                date_de_naissance=date(1995, 5, 20),
                genre="Masculin",
                objectif_principal="Prise de masse",
                poids_actuel=75.5,
                taille_cm=180,
                mot_de_passe_hash="hash_securise_123",
            )
            session.add(new_user)
            session.commit()
            print(
                f"Utilisateur '{new_user.prenom}' créé (ID: {new_user.id_utilisateur})"
            )
        else:
            print("L'utilisateur de test existe déjà en base.")

    except Exception as e:
        session.rollback()
        print(f"Erreur : {e}")
    finally:
        session.close()


if __name__ == "__main__":
    run_test_insert()
