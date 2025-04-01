import streamlit as st
import database  
from queries import *
from queries2 import *

# Configuration de l'interface
st.set_page_config(page_title="Projet Exploration et Interrogation de Bases de Données NoSQL", layout="wide")

# Onglets principaux
tabs = ["Connexions", "MongoDB", "Neo4j","Transverse"]
selected_tab = st.sidebar.radio("Navigation", tabs)

if selected_tab == "Connexions":
    st.header("Connexion aux Bases NoSQL")
    st.write("Ce projet utilise deux bases de données NoSQL : MongoDB et Neo4j.")

    st.write("Tous les résultats sont récupérés et visualisés via Streamlit.")
    st.write("1. Connexion sécurisée à MongoDB (via pymongo).")
     # connexion MongoDB
    try:
        db = database.get_mongo_connection()
       # collections = db.list_collection_names()
        st.success("Connexion à MongoDB réussie")
       # st.write("Collections disponibles :", collections)
    except Exception as e:
        st.error(f"Erreur de connexion à MongoDB : {e}")
    
    st.write("2. Connexion à Neo4j (via un driver Neo4j).")
      # connexion Neo4j
    try:
        driver = database.get_neo4j_connection()
        with driver.session() as session:
            result = session.run("RETURN 'Connexion à Neo4j réussie' AS message")
            for record in result:
                st.success(record["message"])
    except Exception as e:
        st.error(f"Erreur de connexion à Neo4j : {e}")



    

elif selected_tab == "MongoDB":
    st.header("Requêtes MongoDB")
   
    # Question 1: Année avec le plus grand nombre de films
    year_info = most_popular_year()
    st.subheader("1. Année avec le plus grand nombre de films sortis")
    st.write(f"Année : {year_info['_id']} | Nombre de films : {year_info['count']}")

    # Question 2: Nombre de films après 1999
    movies_after_1999 = count_movies_after_1999()
    st.subheader("2. Nombre de films sortis après 1999")
    st.write(f"Nombre de films : {movies_after_1999}")
    
    # Question 3: Moyenne des votes des films sortis en 2007
    avg_votes_2007 = average_votes_2007()
    st.subheader("3. Moyenne des votes des films sortis en 2007")
    st.write(f"Moyenne des votes : {avg_votes_2007}")


    # Question 4: Histogramme du nombre de films par année
    st.subheader("4. Histogramme du nombre de films par année")
    df_histogram = plot_movies_per_year()
    if not df_histogram.empty:
        plt.figure(figsize=(10, 6))
        plt.bar(df_histogram['year'], df_histogram['count'], color='skyblue')
        plt.xlabel('Année')
        plt.ylabel('Nombre de films')
        plt.title('Nombre de films par année')
        st.pyplot(plt)
    else:
        st.write("Aucune donnée disponible pour tracer l'histogramme.")


    # Question 5: Genres de films disponibles
    st.subheader("5. Genres de films disponibles")
    genres = available_genre()
    if genres:
        st.write(", ".join(genres))
    else:
        st.write("Aucun genre disponible.")

    # Question 6: Film ayant généré le plus de revenus
    st.subheader("6. Film ayant généré le plus de revenus")
    result =highest_revenue_movie()
    st.text(result)

    # Question 7: Réalisateurs ayant fait plus de 5 films
    st.subheader("7. Réalisateurs ayant fait plus de 5 films")
    result = directors_with_more_than_5_movies()
    st.text(result)

    # Question 8: Genre rapportant en moyenne le plus de revenus
    st.subheader("8. Genre rapportant en moyenne le plus de revenus")
    result = most_profitable_genre()
    st.text(result)

    # Question 9: Top 3 films par décennie
    st.subheader("9. Top 3 films par décennie")
    result = top_movies_by_decade()
    if isinstance(result , pd.DataFrame):
        st.dataframe(result)
    else:
        st.text(result) 

    # Question 10: Film le plus long par genre
    st.subheader("10. Film le plus long par genre")
    result = longest_movie_per_genre()
    if result is not None:
        st.dataframe(result)  
    else:
        st.write("Aucun film trouvé.")
        

   # Question 11: Créer une vue MongoDB affichant uniquement les films ayant une note supérieure à 80 et généré plus de 50 millions de dollars
    st.subheader("11. Créer une vue MongoDB")
    result = create_high_quality_movies_view()
    st.write(result)

    # Question 12: Calculer la corrélation entre la durée des films (Runtime) et leur revenu (Revenue)
    st.subheader("12. Calculer la corrélation entre la durée des films et leur revenu")

    # Récupérer les données
    df = get_runtime_and_revenue()
    if isinstance(df, pd.DataFrame):
        correlation = calculate_correlation(df)
        st.write(f"Le coefficient de corrélation entre la durée des films et leur revenu est : {correlation:.4f}")
    else:
        st.write(df) 

    st.write(f"Un coefficient de corrélation proche de +1 indique une relation forte et positive, ce qui signifie qu'un film plus long serait fortement associé à un revenu plus élevé. Inversement, un coefficient proche de -1 suggérerait qu'un film plus long serait associé à un revenu plus faible. Si la corrélation est proche de 0, cela signale qu'il n'y a pas de lien linéaire significatif entre la durée des films et leurs revenus.")
  

    # Question 13: Évolution de la durée moyenne des films par décennie
    st.subheader("13. Évolution de la durée moyenne des films par décennie")
    #st.write("Durée moyenne des films par décennie:")
    average_runtime_per_decade_chart(db)
    


elif selected_tab == "Neo4j":
    st.header("Requêtes Neo4j")

    neo4j_driver = get_neo4j_connection()  # Récupère la connexion Neo4j 
    queries = Queries2(neo4j_driver)  # Utilise la connexion pour initialiser Queries2 (requetes neo4j)

    #Question 14: acteur ayant joué dans le plus grand nombre de films ?
    def display_results(query_results):
        if not query_results:
            st.write("Aucun résultat trouvé.")
        else:
            for result in query_results:
                st.write(result)


    # 14. Acteur ayant joué dans le plus grand nombre de films
    st.subheader("14. Acteur ayant joué dans le plus grand nombre de films")
    result = queries.question_14()
    st.write(result)

    # 15. Acteurs ayant joué avec Anne Hathaway
    st.subheader("15. Acteurs ayant joué avec Anne Hathaway")
    result = queries.question_15()
    unique_actors = list(set(result))
    actors_str = ", ".join(unique_actors)
    st.text(actors_str)

    # 16. Acteur ayant joué dans des films avec le plus de revenus
    st.subheader("16. Acteur ayant joué dans des films avec le plus de revenus")
    result = queries.question_16()
    if result:
        actor_name = result[0]['actor']
        total_revenue = result[0]['total_revenue']
        st.write(f"L'acteur ayant joué dans des films totalisant le plus de revenus est **{actor_name}** avec un revenu total de **${total_revenue:,.2f}**")
    else:
        st.write("Aucun acteur trouvé.")

        
    # 17. Moyenne des votes
    st.subheader("17. Moyenne des votes")
    result = queries.question_17()
    if result:
        avg_votes = result[0].get('avg_votes', 'Donnée non disponible')
        st.write(f"Moyenne des votes (arrondie): {avg_votes}")
    else:
        st.write("Aucune donnée disponible pour calculer la moyenne des votes.")

    # 18. Genre le plus représenté
    st.subheader("18. Genre le plus représenté")
    result = queries.question_18()
    if result:
        genre, count = result[0]  
        st.write(f"Le genre le plus représenté est : {genre} avec {count} films.")
    else:
        st.write("Aucun genre trouvé dans la base de données.")

    # 19. Films dans lesquels les acteurs ayant joué avec nous (Asma et Nazeli) ont joué 
    st.subheader("19.  Films dans lesquels les acteurs ayant joué avec nous ont joué")
    result = queries.question_19()
    if result:
        for record in result:
            st.write(f"Film: {record['film']}, Acteur: {record['actor']}")
    else:
        st.write("Aucun film trouvé.")


    # 20. Réalisateur ayant travaillé avec le plus grand nombre d'acteurs distincts
    st.subheader("20. Réalisateur ayant travaillé avec le plus grand nombre d'acteurs distincts")
    result = queries.question_20()
    if result:
        st.write(f" Réalisateur ayant travaillé avec le plus grand nombre d'acteurs distincts  ( {result[0]['NumberOfActors']} ) est  {result[0]['Director']}")

    else:
        st.write("Aucun résultat trouvé.")
    


    # 21. Films les plus connectés
    st.subheader("21. Films les plus connectés")
    result_21 = queries.question_21()
    if result_21:
        for record in result_21:
            st.write(f" {record['film1']} et {record['film2']}")
            st.write(f"Nombre d'acteurs communs : {record['common_actors']}")
    else:
        st.write("Aucun film trouvé.")


    # 22. Acteurs ayant joué avec le plus grand nombre de réalisateurs
    st.subheader("22. Acteurs ayant joué avec le plus grand nombre de réalisateurs")
    result = queries.question_22()
    df = pd.DataFrame(result, columns=["Actor", "NumberOfDirectors"])
    st.write(df)

    # 23. Recommander un film à un acteur en fonction des genres
    st.subheader("23. Recommander un film à un acteur en fonction des genres")
    st.write("Exemple pour Aaron Eckhart :")
   
    recommendations = queries.question_23()

    if recommendations and "message" in recommendations[0]:
        st.write(recommendations[0]["message"])  
    else:
        for rec in recommendations:
            st.write(f"**{rec['actor']}** pourrait aimer le film : **{rec['recommended_movie']}** (Genres correspondants : {rec['matched_genres']})")


    # 24. Créer une relation INFLUENCE PAR entre réalisateurs
    st.subheader("24. Créer une relation INFLUENCE PAR entre réalisateurs")
    st.write("Relation crée")
  

    # 25. Le chemin le plus court entre deux acteurs
    # Ici on utilise l'algorithme du plus court chemin (shortest path)

    st.subheader("25. Le chemin le plus court entre deux acteurs")
    actor1 = st.text_input("Nom du premier acteur (ex : Tom Hanks)")
    actor2 = st.text_input("Nom du deuxième acteur (ex : Scarlett Johansson)")

    if actor1 and actor2:
        path = queries.question_25(actor1, actor2)
        if isinstance(path, list):
            st.write(f"Le chemin le plus court entre {actor1} et {actor2} :")
            st.write(" -> ".join(path))
        else:
            st.write(path)  
    else:
        st.write("Veuillez entrer les noms de deux acteurs.")


    # # Question 26: Détection de communautés d’acteurs
    # actor_communities_result = actor_communities()
    # st.subheader("26. Communautés d’acteurs qui travaillent souvent ensemble")
    # st.write(actor_communities_result)

if selected_tab == "Transverse":
    st.header("Questions transverses")