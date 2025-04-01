from database import get_mongo_connection, get_neo4j_connection
from neo4j import GraphDatabase

# Extraire les données de MongoDB
def get_movies_data():
    db = get_mongo_connection()
    pipeline = [
        {"$project": {
            "_id": 1,
            "_rev": 1,
            "title": 1,
            "genre": 1,
            "Description": 1,
            "Director": 1,
            "Actors": 1,
            "year": 1,
            "rating": 1,
            "Votes": 1,
            "Revenue (Millions)": 1,
            "Metascore": 1
        }},
        {"$match": {"year": {"$gte": 1900}}}  
    ]
    movies = list(db.films.aggregate(pipeline))
    return movies

#  Créer un nœud pour le film
def create_film_node(driver, movie):
    with driver.session() as session:
        session.run(
            """
            CREATE (f:Film {
                id: $id,
                title: $title,
                year: $year,
                description: $description,
                rating: $rating,
                votes: $votes,
                revenue: $revenue,
                metascore: $metascore,
                director: $director
            })
            """,
            id=str(movie["_id"]),
            title=movie["title"],
            year=movie["year"],
            description=movie["Description"],
            rating=movie["rating"],
            votes=movie["Votes"],
            revenue=movie["Revenue (Millions)"],
            metascore=movie["Metascore"],
            director=movie["Director"]
        )

# Créer un nœud pour les acteurs et leur relation avec les films
def create_actor_node(driver, actor, film_id):
    with driver.session() as session:
        session.run(
            """
            MERGE (a:Actor {name: $actor_name})
            WITH a
            MATCH (f:Film {id: $film_id})
            MERGE (a)-[:ACTED_IN]->(f)
            """,
            actor_name=actor,
            film_id=str(film_id)
        )


# Créer des relations entre les films et les genres
def create_genre_relations(driver, movie, genres):
    if genres: 
        with driver.session() as session:
            for genre in genres:
           #     print(f"Creating genre: {genre}") 
                session.run(
                    """
                    MATCH (f:Film {id: $film_id})
                    MERGE (g:Genre {name: $genre})
                    MERGE (f)-[:HAS_GENRE]->(g)
                    """,
                    film_id=str(movie["_id"]),
                    genre=genre.strip()  
                )
    else:
        print(f"No genres for movie: {movie['title']}")


#  Créer un nœud pour le réalisateur
def create_director_node(driver, director):
    with driver.session() as session:
        session.run(
            """
            MERGE (d:Director {name: $director_name})
            """,
            director_name=director
        )

#  Ajouter Asma et Nazeli au film "Split"
def add_actors_to_split(driver):
    with driver.session() as session:
        session.run(
            """
            MATCH (f:Film {title: "Split"})
            MERGE (a1:Actor {name: "Asma"})
            MERGE (a2:Actor {name: "Nazeli"})
            MERGE (a1)-[:ACTED_IN]->(f)
            MERGE (a2)-[:ACTED_IN]->(f)
            """
        )


#  Relation entre les acteurs et réalisateur
def create_directed_relation(driver, director, actor):
    with driver.session() as session:
        session.run(
            """
            MATCH (d:Director {name: $director_name})
            MATCH (a:Actor {name: $actor_name})
            MERGE (d)-[:DIRECTED]->(a)
            """,
            director_name=director,
            actor_name=actor
        )


# Relation entre les acteurs et le genre de films dans lesquels ils ont joué
def create_actor_genre_relationship(driver):
    with driver.session() as session:
        session.run(
            """
            MATCH (a:Actor)-[:ACTED_IN]->(f:Film)-[:HAS_GENRE]->(g:Genre)
            MERGE (a)-[:LIKES_GENRE]->(g)
            """
        )


# Fonction pour créer la relation DIRECTED entre les réalisateurs et les films
def create_directed_relationships(driver):    
    with driver.session() as session:
        session.run(
            """
            MATCH (f:Film)
            WHERE f.director IS NOT NULL
            MERGE (d:Director {name: f.director})
            MERGE (d)-[:DIRECTED]->(f)
            """
        )

# relation INFLUENCE PAR entre les réalisateurs en se basant sur des similarités dans les genres de films qu’ils ont réalisés.
def create_influence_relationship(driver):
    with driver.session() as session:
        session.run(
            """
            MATCH (d1:Director)-[:DIRECTED]->(f1:Film)-[:HAS_GENRE]->(g:Genre),
                  (d2:Director)-[:DIRECTED]->(f2:Film)-[:HAS_GENRE]->(g)
            WHERE d1 <> d2 
            WITH d1, d2, COUNT(DISTINCT g) AS common_genres
            MERGE (d1)-[:INFLUENCE_PAR]->(d2)
            """
        )


# Fonction principale pour importer les données depuis MongoDB vers Neo4j
def import_data_to_neo4j():
   
    neo4j_driver = get_neo4j_connection()

    movies = get_movies_data()
    for movie in movies:
        create_film_node(neo4j_driver, movie)
        create_director_node(neo4j_driver, movie["Director"])
        genres = movie["genre"].split(",") if "genre" in movie else []
        create_genre_relations(neo4j_driver, movie, genres)

    for movie in movies:
        actors = movie["Actors"].split(",") if "Actors" in movie else []
        for actor in actors:
            create_actor_node(neo4j_driver, actor.strip(), movie["_id"])

            create_directed_relation(neo4j_driver, movie["Director"], actor.strip())

    add_actors_to_split(neo4j_driver)
    create_actor_genre_relationship(neo4j_driver)
    create_directed_relationships(neo4j_driver)
    create_influence_relationship(neo4j_driver)

if __name__ == "__main__":
    import_data_to_neo4j()
