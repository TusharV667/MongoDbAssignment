from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb://127.0.0.1:27017/"   #Replace this with your mongo URI

# Function to establish MongoDB connection
def connect_to_mongodb():
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")

        db = client['sample_mflix']  
        return (db)
    
    except Exception as e:
        print(e)

