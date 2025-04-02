Projet NoSQL

Ce projet explore l'utilisation des bases de données NoSQL, notamment MongoDB et Neo4j, pour analyser et manipuler des données cinématographiques. 

Technologies utilisées
MongoDB : Stockage et interrogation des données sur les films.
Neo4j : Analyse des relations entre acteurs et films.
Streamlit : Création d'une interface utilisateur interactive pour afficher les résultats.
Python : Langage principal pour les scripts et l'interfaçage.

Installation

1. Cloner le dépôt
   
       git clone https://github.com/eloijaji/NoSQL.git
       cd NoSQL

3. Créer un environnement virtuel

       python -m venv venv
    Activer l'environnement virtuel :

          Windows : venv\Scripts\activate
          Mac/Linux : source venv/bin/activate

5. Installer les dépendances

       pip install -r requirements.txt

Utilisation
        Lancer l'application Streamlit avec la commande 
        
          streamlit run appli.py
