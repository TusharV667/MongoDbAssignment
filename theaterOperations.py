def top_10_cities_with_max_theaters(db):
    pipeline = [
        {"$group": {"_id": "$location.address.city", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = db.theaters.aggregate(pipeline)
    return result

#--------------------------------------------------------------------------------------------------------------------
def top_10_theaters_nearby(coordinates, db):
    pipeline = [
        {"$geoNear": {
            "near": {"type": "Point", "coordinates": coordinates},
            "distanceField": "distance",
            "spherical": True
        }},
        {"$group": {"_id": "$location.address.city"}},
        {"$limit": 10}
    ]
    result = db.theaters.aggregate(pipeline)
    return [doc["_id"] for doc in result]