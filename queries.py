from database import get_mongo_connection
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st
import seaborn as sns
import altair as alt

# Connexion à MongoDB
db = get_mongo_connection()
films = db.films


# 1. Afficher l'année où le plus grand nombre de films ont été sortis

def most_popular_year():
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    result = list(films.aggregate(pipeline))
    if result:
      #  print(f"Année avec le plus de films : {result[0]['_id']} ({result[0]['count']} films)")
        return result[0]  
    return None 


# 2. Nombre de films sortis après 1999

def count_movies_after_1999():
    try:
        count = films.count_documents({"year": {"$gt": 1999}})
     #   print(f"Nombre de films après 1999 : {count}")
        return count  
    except Exception as e:
        print(f"Erreur lors de la récupération des films : {e}")
        return None

# 3. Moyenne des votes des films sortis en 2007

def average_votes_2007():
    pipeline = [
        {"$match": {"year": 2007}},
        {"$group": {"_id": None, "avg_votes": {"$avg": "$Votes"}}}
    ]
    result = list(films.aggregate(pipeline))
    return result[0]['avg_votes'] if result else 'Non disponible'


# Question 4: Histogramme du nombre de films par année
def plot_movies_per_year():
    try:
        db = get_mongo_connection()
        pipeline = [
            {"$group": {"_id": "$year", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        data = list(db.films.aggregate(pipeline))
        years_counts = [(int(doc["_id"]), doc["count"]) for doc in data if doc["_id"] is not None]
        if not years_counts:
            st.write("Aucune donnée disponible pour tracer l'histogramme.")
            return pd.DataFrame() 
        df = pd.DataFrame(years_counts, columns=["year", "count"])      
        return df
    except Exception as e:
        st.error(f"Erreur lors de la récupération des films par année : {e}")
        return pd.DataFrame()  

# 5. Genres de films disponibles

def available_genre():
    db = get_mongo_connection()
    genres_cursor = db.films.distinct("genre")  

    unique_genres = set()
    for genre_entry in genres_cursor:
        if genre_entry: 
            genres_split = genre_entry.split(",") 
            unique_genres.update(genres_split)  
    unique_genres = sorted(unique_genres) 
    return unique_genres  

# 6. Film ayant généré le plus de revenus
def highest_revenue_movie():
    db = get_mongo_connection()
    movie = db.films.find_one(sort=[("Revenue (Millions)", -1)])
    if movie:
        return f"Film avec le plus de revenus : {movie['title']} ({movie['Revenue (Millions)']}M$)"
    return "Aucun film trouvé avec des revenus."



# 7. Réalisateurs ayant fait plus de 5 films
def directors_with_more_than_5_movies():
 
    pipeline = [
        {"$unwind": {"path": "$Director", "preserveNullAndEmptyArrays": True}}, 
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 5}}},  
        {"$sort": {"count": -1}}  
    ]
    try:
        results = list(db.films.aggregate(pipeline))
        if not results:
            return "Aucun réalisateur n'a plus de 5 films dans la base."
        directors = [f"{director['_id']} ({director['count']} films)" for director in results]
        return "\n".join(directors)
    except Exception as e:
        return f"Erreur dans la requête MongoDB : {e}"



# 8. Genre rapportant en moyenne le plus de revenus

def most_profitable_genre():
    db = get_mongo_connection()  
    
    if db is None:
        return "Erreur de connexion à la base de données."
    pipeline = [
        {"$group": {"_id": "$genre", "avg_revenue": {"$avg": "$Revenue (Millions)"}}}, 
        {"$sort": {"avg_revenue": -1}}, 
        {"$limit": 1}  
    ]
    
    try:
        result = list(db.films.aggregate(pipeline))
        if result:
            genre = result[0]['_id']
            avg_revenue = result[0]['avg_revenue']
            return f"Genre le plus rentable : {genre} ({avg_revenue}M$ en moyenne)"
        else:
            return "Aucun genre trouvé dans la base de données."

    except Exception as e:
        return f"Erreur dans la requête MongoDB : {e}"

# 9. Top 3 films par décennie


def top_movies_by_decade():
    db = get_mongo_connection()  
    
    if db is None:
        return "Erreur de connexion à la base de données."
    
    decades = [(1990, 1999), (2000, 2009), (2010, 2019)]
    
    results = {"Décennie": [], "Film 1": [], "Film 2": [], "Film 3": []}  
    
    for start_year, end_year in decades:
        pipeline = [
            {"$match": {"year": {"$gte": start_year, "$lte": end_year}, "rating": {"$ne": None}}}, 
            {"$sort": {"rating": -1}},  
            {"$limit": 3},  
            {"$project": {"title": 1}}  
        ]
     
        try:
            decade_results = list(db.films.aggregate(pipeline))
            results["Décennie"].append(f"{start_year}-{end_year}")
            if decade_results:
                for i in range(3):
                    if i < len(decade_results):
                        results[f"Film {i + 1}"].append(decade_results[i]["title"])
                    else:
                        results[f"Film {i + 1}"].append("Aucun film trouvé")
            else:
            
                for i in range(3):
                    results[f"Film {i + 1}"].append("Aucun film trouvé")
        
        except Exception as e:
            return f"Erreur dans la requête MongoDB : {e}"
    return pd.DataFrame(results)

# 10. Film le plus long par genre

def longest_movie_per_genre():
    db = get_mongo_connection()

    pipeline = [
        {"$match": {
            "Runtime (Minutes)": {"$ne": None, "$type": "int"},
            "genre": {"$exists": True, "$type": "string"}
        }},
        {"$addFields": {
            "genres": {
                "$map": {
                    "input": {"$split": ["$genre", ","]},
                    "as": "g",
                    "in": {"$trim": {"input": "$$g"}}
                }
            }
        }},
        {"$unwind": "$genres"},
        {"$sort": {"Runtime (Minutes)": -1}},
        {"$group": {
            "_id": "$genres",
            "longest_movie": {"$first": "$title"},
            "longest_runtime": {"$first": "$Runtime (Minutes)"}
        }},
        {"$sort": {"longest_runtime": -1}}
    ]
    
    results = list(db.films.aggregate(pipeline))
    
    if results:
        data = {"Genre": [], "Longest Movie": [], "Duration (Minutes)": []}
        for genre in results:
            data["Genre"].append(genre['_id'])
            data["Longest Movie"].append(genre['longest_movie'])
            data["Duration (Minutes)"].append(genre['longest_runtime'])
        return pd.DataFrame(data)
    else:
        return None



# 11. Créer une vue MongoDB affichant uniquement les films ayant une note superieure à 80 et généré plus de 50millions de dollars


def create_high_quality_movies_view():
    try:
        #  films ayant une note > 80 et revenus > 50 millions
        db.command("create", "high_quality_movies", viewOn="films", pipeline=[
            {"$match": {"Metascore": {"$gt": 80}, "Revenue (Millions)": {"$gt": 50}}}
        ])
        return "Vue 'high_quality_movies' créée avec succès."
    except Exception as e:
        return f"Erreur lors de la création de la vue : {e}"

# 12. Corrélation entre durée et revenu

def get_runtime_and_revenue():
    db = get_mongo_connection()  

    pipeline = [
        {"$match": {"Runtime (Minutes)": {"$ne": None, "$type": "int"},
                    "Revenue (Millions)": {"$ne": None, "$type": "double"}}}, 
        {"$project": {"Runtime (Minutes)": 1, "Revenue (Millions)": 1}} 
    ]
 
    results = list(db.films.aggregate(pipeline))
    
    if not results:
        return "Aucun film trouvé avec des données valides."
    df = pd.DataFrame(results)
    
    return df


def calculate_correlation(df):
    if df.empty:
        return "Les données sont vides, impossible de calculer la corrélation."

    correlation = df["Runtime (Minutes)"].corr(df["Revenue (Millions)"])
    
    return correlation



# 13. Évolution de la durée moyenne des films par décennie

def average_runtime_per_decade_chart(db):
    pipeline = [
       
        {"$match": {
            "Runtime (Minutes)": {"$ne": None, "$type": "int"},
            "year": {"$ne": None, "$type": "int"}  
        }},
    
        {"$addFields": {
            "decade": {"$floor": {"$divide": ["$year", 10]}}
        }},
       
        {"$group": {
            "_id": "$decade",
            "average_runtime": {"$avg": "$Runtime (Minutes)"}
        }},
       
        {"$sort": {"_id": 1}}
    ]
    
    results = list(db.films.aggregate(pipeline))
    
    if not results:
        print("Aucun film trouvé.")
        return

    decades = [f"{decade['_id']*10}s" for decade in results]
    average_runtimes = [decade['average_runtime'] for decade in results]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(decades, average_runtimes, color='skyblue')

    ax.set_xlabel('Décennie')
    ax.set_ylabel('Durée moyenne des films (minutes)')
    ax.set_title('Durée moyenne des films par décennie')
    st.pyplot(fig)


# 27. films ayant des genres en commun mais qui ont des réalisateurs différent
def question_27():
        pipeline = [
            {"$unwind": {"path": "$genre", "preserveNullAndEmptyArrays": True}},  
            {"$group": {
                "_id": "$genre",
                "directors": {"$addToSet": "$Director"}, 
                "films": {"$push": {"title": "$title", "director": "$Director"}}
            }},
            {"$match": {"directors.1": {"$exists": True}}},  
            {"$sort": {"films": -1}},  
            {"$limit": 5} 
        ]

        result = list(films.aggregate(pipeline))
        
        if result:
            return result
        return "Aucun genre n'est partagé par plusieurs réalisateurs."



# if __name__ == "__main__":
#     most_popular_year()
#     count_movies_after_1999()
#     average_votes_2007()
#     plot_movies_per_year()
#     available_genre()
#     highest_revenue_movie()
#     directors_with_more_than_5_movies()
#     most_profitable_genre()
#     top_movies_by_decade()
#     longest_movie_per_genre()
#     create_high_quality_movies_view()
#   #  runtime_revenue_correlation()
#   #  average_runtime_per_decade()
