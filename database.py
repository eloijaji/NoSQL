#Connexion à MongoDB
from pymongo import MongoClient

MONGO_URI = "mongodb+srv://eloijaji:projetSQL@projetsql.tpeii.mongodb.net/?retryWrites=true&w=majority&appName=ProjetSQL"
client = MongoClient(MONGO_URI, tls=True, tlsAllowInvalidCertificates=False)
db = client["movies"]
print("Collections disponibles : ", db.list_collection_names())
collection = db["ma_collection"]  
documents = collection.find()

for doc in documents:
    print(doc)


#Connexion à Neo4j