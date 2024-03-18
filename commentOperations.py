from datetime import datetime

#--------------------------------------------------------------------------------------------------------------------
def top_10_commenters(db):

    pipeline = [
        {"$group": {"_id": {"name": "$name", "email": "$email"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = db.comments.aggregate(pipeline)
    top_commenters = [item["_id"]["name"] for item in result]
    return top_commenters

#--------------------------------------------------------------------------------------------------------------------
def top_10_commented_movies(db):
    pipeline = [
        {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    movie_ids = [item["_id"] for item in db.comments.aggregate(pipeline)]
    
    # Lookup movies collection to get movie titles
    pipeline_movies = [
        {"$match": {"_id": {"$in": movie_ids}}},
        {"$project": {"_id": 1, "title": 1}}
    ]
    # Execute the aggregation pipeline
    movies = list(db.movies.aggregate(pipeline_movies))
    # Create a dictionary to map movie IDs to titles
    movie_titles = {movie["_id"]: movie["title"] for movie in movies}
    # Assign movie titles to the result
    result = [{"title": movie_titles[item["_id"]], "count": item["count"]} for item in db.comments.aggregate(pipeline)] 
    return result

#--------------------------------------------------------------------------------------------------------------------
def comments_per_month_in_year(db, year):
    pipeline = [
        {
            "$match": {
                "date": {
                    "$gte": datetime(year, 1, 1),  # Start of the year
                    "$lt": datetime(year + 1, 1, 1)  # Start of the next year
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$date"},
                    "month": {"$month": "$date"}
                },
                "total_comments": {"$sum": 1}
            }
        },
        {
            "$sort": {"_id.year": 1, "_id.month": 1}
        }
    ]

    return list(db.comments.aggregate(pipeline))

#--------------------------------------------------------------------------------------------------------------------


