from database import get_mongo_connection, get_neo4j_connection
from neo4j import GraphDatabase

class Queries2:
       
    def __init__(self, driver):
        self.driver = driver  

    def execute_query(self, query):
        with self.driver.session() as session:
            result = session.run(query)
            return [record for record in result]


    def question_14(self):
        query = """
        MATCH (a:Actor)-[:ACTED_IN]->(f:Film)
        RETURN a.name AS actor, COUNT(f) AS film_count
        ORDER BY film_count DESC
        LIMIT 1
        """
        results = self.execute_query(query)
        
        if results:
            actor = results[0]['actor']
            film_count = results[0]['film_count']
            return f"L'acteur ayant joué dans le plus grand nombre de films est {actor} avec {film_count} films."
        else:
            return "Aucun résultat trouvé."


    # 15. Quels sont les acteurs ayant joué dans des films où l’actrice Anne Hathaway a également joué ?
    def question_15(self):
        query = """
        MATCH (anne:Actor {name: 'Anne Hathaway'})-[:ACTED_IN]->(movie:Film)<-[:ACTED_IN]-(actor:Actor)
        WHERE actor.name <> 'Anne Hathaway'
        RETURN actor.name AS actor_name
        """
        result = self.execute_query(query)
        actors = [record['actor_name'] for record in result]
        return actors


    # 16. Quel est l’acteur ayant joué dans des films totalisant le plus de revenus ?
    def question_16(self):
        query = """
        MATCH (a:Actor)-[:ACTED_IN]->(f:Film)
        WHERE f.revenue IS NOT NULL
        RETURN a.name AS actor, SUM(toFloat(f.revenue)) AS total_revenue
        ORDER BY total_revenue DESC
        LIMIT 1
        """
        return self.execute_query(query)


    # 17. Quelle est la moyenne des votes ?
    def question_17(self):
        query = """
        MATCH (f:Film)
        RETURN ROUND(AVG(toFloat(f.votes))) AS avg_votes
        """
        return self.execute_query(query)

    # 18. Quel est le genre le plus représenté dans la base de données ?
    def question_18(self):
        query = """
        MATCH (f:Film)-[:HAS_GENRE]->(g:Genre)
        RETURN g.name AS genre, COUNT(f) AS count
        ORDER BY count DESC
        LIMIT 1;
        """
        return self.execute_query(query)

    # 19. Quels sont les films dans lesquels les acteurs ayant joué avec vous ont également joué ?
    def question_19(self):
        query = """
        MATCH (a1:Actor)-[:ACTED_IN]->(f:Film)<-[:ACTED_IN]-(a2:Actor)
        WHERE a1.name IN ['Asma', 'Nazeli']
        AND a2.name <> 'Asma' AND a2.name <> 'Nazeli'
        WITH DISTINCT a2
        MATCH (a2)-[:ACTED_IN]->(f2:Film)
        RETURN DISTINCT f2.title AS film, a2.name AS actor
        """
        return self.execute_query(query)

     # 20. Quel réalisateur a travaillé avec le plus grand nombre d’acteurs distincts ?
    def question_20(self):
        query = """
        MATCH (d:Director)-[:DIRECTED]->(a:Actor)
        WITH d, COUNT(DISTINCT a) AS actor_count
        ORDER BY actor_count DESC
        LIMIT 1
        RETURN d.name AS Director, actor_count AS NumberOfActors
        """
        return self.execute_query(query)

    

    # 21. Quels sont les films les plus "connectés", c’est-à-dire ceux qui ont le plus d’acteurs en commun avec d’autres films ?
    def question_21(self):
        query = """
        MATCH (f1:Film)<-[:ACTED_IN]-(a:Actor)-[:ACTED_IN]->(f2:Film)
        WHERE f1 <> f2  // Assure que f1 et f2 sont distincts
        AND f1.title < f2.title  // Garantit que chaque paire de films est unique
        RETURN f1.title AS film1, f2.title AS film2, COUNT(a) AS common_actors
        ORDER BY common_actors DESC
        LIMIT 1
        """
        return self.execute_query(query)

    # 22. Trouver les 5 acteurs ayant joué avec le plus de réalisateurs différents.
    def question_22(self):
        query = """
        MATCH (d:Director)-[:DIRECTED]->(a:Actor)
        WITH a, COUNT(DISTINCT d) AS director_count
        ORDER BY director_count DESC
        LIMIT 5
        RETURN a.name AS Actor, director_count AS NumberOfDirectors
        """
        return self.execute_query(query)

    # 23. Recommander un film à un acteur en fonction des genres des films où il a déjà joué.
    def question_23(self):
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (a:Actor)-[:ACTED_IN]->(f:Film)-[:HAS_GENRE]->(g:Genre)
                WITH a, COLLECT(DISTINCT g) AS genres
                MATCH (f2:Film)-[:HAS_GENRE]->(g)
                WHERE NOT (a)-[:ACTED_IN]->(f2) 
                RETURN a.name AS actor, f2.title AS recommended_movie, COUNT(DISTINCT g) AS matched_genres
                ORDER BY actor, matched_genres DESC
                LIMIT 3
                """
            )
            
            recommendations = result.data()
            
            if recommendations:
                return recommendations
            else:
                return [{"message": "Aucune recommandation trouvée."}]



    # 24. Créer une relation INFLUENCE PAR entre les réalisateurs en se basant sur des similarités dans les genres de films qu’ils ont réalisés.
    # voir fonction create_influence_relationship dans integration.py 

    # 25. Quel est le "chemin" le plus court entre deux acteurs donnés ?
    # def question_25(actor1, actor2):
    #     driver = get_neo4j_connection()

    #     with driver.session() as session:
    #         result = session.run(
    #             """
    #             MATCH (a1:Actor {name: $actor1}), (a2:Actor {name: $actor2}),
    #                 p = shortestPath((a1)-[:ACTED_IN*]-(a2))
    #             RETURN p
    #             """,
    #             actor1=actor1,
    #             actor2=actor2
    #         )
            
    #         # Traiter les résultats
    #         path = result.single()
    #         if path:
    #             # Afficher le chemin trouvé sous forme de liste de nœuds
    #             return [str(node['name']) for node in path["p"].nodes]
    #         else:
    #             return "Aucun chemin trouvé entre ces deux acteurs."
        
    # def question_25(self, actor1, actor2):
    #     with self.driver.session() as session:
    #         result = session.run(
    #             """
    #             MATCH (a1:Actor {name: $actor1}), (a2:Actor {name: $actor2}),
    #                 p = shortestPath((a1)-[:ACTED_IN*]-(a2))
    #             RETURN p
    #             """,
    #             actor1=actor1,
    #             actor2=actor2
    #         )

    # def question_25(self, actor1, actor2):
    #     with self.driver.session() as session:
    #         # Exécution de la requête pour obtenir le chemin le plus court
    #         result = session.run(
    #             """
    #             MATCH (a1:Actor {name: $actor1}), (a2:Actor {name: $actor2}),
    #                 p = shortestPath((a1)-[:ACTED_IN*]-(a2))
    #             RETURN p
    #             """,
    #             actor1=actor1,
    #             actor2=actor2
    #         )
            
    #         # Récupérer le chemin
    #         path = result.single()  # Récupère le premier (et probablement seul) résultat
    #         if path:
    #             # Extraire les noms des nœuds dans le chemin et les retourner sous forme de liste
    #             return [str(node['name']) for node in path["p"].nodes]
    #         else:
    #             # Retourne un message si aucun chemin n'est trouvé
    #             return "Aucun chemin trouvé entre ces deux acteurs."


    # def question_25(self, actor1, actor2):
    #     with self.driver.session() as session:
    #         # Exécution de la requête pour obtenir le chemin le plus court
    #         result = session.run(
    #             """
    #             MATCH (a1:Actor {name: $actor1}), (a2:Actor {name: $actor2}),
    #                 p = shortestPath((a1)-[:ACTED_IN*]-(a2))
    #             RETURN p
    #             """,
    #             actor1=actor1,
    #             actor2=actor2
    #         )
            
    #         # Récupérer le chemin
    #         path = result.single()  # Récupère le premier (et probablement seul) résultat
    #         if path:
    #             # Filtrer les nœuds None et extraire les noms des nœuds dans le chemin
    #             nodes = [str(node['name']) for node in path["p"].nodes if node]
    #             if nodes:
    #                 return nodes
    #             else:
    #                 return "Aucun chemin valide trouvé entre ces deux acteurs."
    #         else:
    #             return "Aucun chemin trouvé entre ces deux acteurs."


    def question_25(self, actor1, actor2):
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (a1:Actor {name: $actor1}), (a2:Actor {name: $actor2}),
                    p = shortestPath((a1)-[:ACTED_IN*]-(a2))
                RETURN p
                """,
                actor1=actor1,
                actor2=actor2
            )
            
            path = result.single()  
            if path:
                nodes = []
                for node in path["p"].nodes:
                    if node.labels == ['Film']: 
                        nodes.append(str(node['title']))  
                    elif node.labels == ['Actor']: 
                        nodes.append(str(node['name']))  
                if nodes:
                    return nodes
                else:
                    return "Aucun chemin valide trouvé entre ces deux acteurs."
            else:
                return "Aucun chemin trouvé entre ces deux acteurs."

    # # 26. Analyser les communautés d’acteurs : Quels sont les groupes d’acteurs qui ont tendance à travailler ensemble ?
    # def question_26(self):
    #     query = """
    #     CALL algo.louvain.stream('Actor', 'ACTED_IN', {graph:'movie_graph'})
    #     YIELD nodeId, community
    #     MATCH (a:Actor) WHERE ID(a) = nodeId
    #     RETURN community, COLLECT(a.name) AS actors
    #     ORDER BY community
    #     """
    #     return self.execute_query(query)
