from datetime import date
from data_pipeline.database import SessionLocal, engine, Base
from data_pipeline.models import Utilisateur, ProfilSante


def run_test_insert():
    session = SessionLocal()
    try:
        print("🔍 Vérification de l'utilisateur de test...")

        user_email = "jordan.test@healthai.com"
        existing_user = session.query(Utilisateur).filter_by(email=user_email).first()

        if not existing_user:
            #  Création de l'entité de l'utilisateur
            new_user = Utilisateur(
                nom="Nkunga",
                prenom="Jordan",
                email=user_email,
                date_de_naissance=date(1995, 5, 20),
                genre="Masculin",
                mot_de_passe_hash="hash_securise_123",
            )

            #  Création profil santé lié à l'utilisateur
            ProfilSante(
                objectif_principal="Prise de masse",
                poids_kg=75.5,
                taille_cm=180,
                imc=23.3,
                utilisateur=new_user,  # Le lien est fait ici
            )

            session.add(new_user)  # Ajoute aussi le profil grâce à la relation
            session.commit()
            print(f" Utilisateur '{new_user.prenom}' et son Profil Santé créés !")
        else:
            print(f"L'utilisateur {user_email} existe déjà.")

    except Exception as e:
        session.rollback()
        print(f"Erreur : {e}")
    finally:
        session.close()


if __name__ == "__main__":
    print("♻️ Réinitialisation de la base de données...")
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    run_test_insert()
