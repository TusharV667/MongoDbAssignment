import json
import bson
import bson.json_util
from pymongo import errors

# Function to bulk load JSON data into MongoDB collections
def bulk_load_collections(db):
    collections = ['comments', 'movies', 'users', 'theaters']
    for collection_name in collections:
        with open(f'Jsons/{collection_name}.json', 'r') as file:
            # Iterate over each line in the file
            for line in file:
                    data = json.loads(line)
                    bson_data = bson.json_util.loads(bson.json_util.dumps(data))

                    # Insert JSON data into MongoDB collection
                    try:
                        db[collection_name].insert_one(bson_data)
                    except errors.BulkWriteError as e:
                        print(f"Error inserting document into collection '{collection_name}': {e}")

                # Print the total number of documents inserted for the collection
            print(f"Inserted {len(data)} documents into collection '{collection_name}'")
