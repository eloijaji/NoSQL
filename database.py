from pymongo import MongoClient
from neo4j import GraphDatabase
import config  

# Connexion à MongoDB
def get_mongo_connection():
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB_NAME]
    return db

# Connexion à Neo4j
def get_neo4j_connection():
    driver = GraphDatabase.driver(config.NEO4J_URI, auth=(config.NEO4J_USERNAME, config.NEO4J_PASSWORD))
    return driver

# Tests de connexion
if __name__ == "__main__":
    # Test MongoDB
    db = get_mongo_connection()
    print("Connexion MongoDB réussie :", db.list_collection_names())

    # Test Neo4j
    driver = get_neo4j_connection()
    print("Connexion Neo4j réussie.")

