def top_n_movies_highest_imdb_rating(N, db):
    pipeline = [
        {"$match":{"imdb.rating": {"$ne": ''}}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N}
    ]
    return list(db.movies.aggregate(pipeline))

#--------------------------------------------------------------------------------------------------------------------
def print_top_n_movies_highest_imdb_rating_in_year(N, year, db):
    pipeline = [
        {"$match": {"year": year}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N}
    ]
    return db.movies.aggregate(pipeline)

#--------------------------------------------------------------------------------------------------------------------
def print_top_n_movies_highest_imdb_rating_votes(N, db):
    pipeline = [
        {"$match": {"imdb.votes": {"$gt": 1000}}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N}
    ]
    return db.movies.aggregate(pipeline)

#--------------------------------------------------------------------------------------------------------------------
def top_n_movies_title_matching_pattern_sorted_by_tomatoes(N, pattern, db):

    pipeline = [
        {"$match": {"title": {"$regex": pattern, "$options": "i"}}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": N}
    ]
    return db.movies.aggregate(pipeline)

#--------------------------------------------------------------------------------------------------------------------
def top_n_directors_most_movies(N, db):
    pipeline = [
        {"$match": {"directors": {"$ne": None}}},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return [entry["_id"] for entry in result]

#--------------------------------------------------------------------------------------------------------------------
def top_n_directors_most_movies_in_year(N, year, db):
    pipeline = [
        {"$match": {"year": year, "directors": {"$ne": None}}},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return [entry["_id"] for entry in result]

#--------------------------------------------------------------------------------------------------------------------
def top_n_directors_most_movies_in_genre(N, genre, db):
    pipeline = [
        {"$match": {"genres": genre, "directors": {"$ne": None}}},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return [entry["_id"] for entry in result]

#--------------------------------------------------------------------------------------------------------------------
def top_n_actors_most_movies(N, db):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return result

#--------------------------------------------------------------------------------------------------------------------
def top_n_actors_most_movies_in_year(N, year, db):
    pipeline = [
        {"$match": {"year": year}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return result

#-------------------------------------------------------------------------------------------------------------------
def top_n_actors_most_movies_in_genre(N, genre, db):
    pipeline = [
        {"$match": {"genres": genre}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ]
    result = db.movies.aggregate(pipeline)
    return result

#--------------------------------------------------------------------------------------------------------------------
def top_n_movies_by_genre_with_highest_imdb(N, db):
    pipeline = [
            {"$unwind": "$genres"},
            {"$match": {"imdb.rating": {"$exists": True, "$ne": ""}}},
            {"$sort": {"genres": 1, "imdb.rating": -1}},
            {"$group": {"_id": "$genres", "top_movies": {"$push": {"title": "$title", "imdb_rating": "$imdb.rating"}}}},
            {"$project": {"_id": 0, "genre": "$_id", "top_movies": {"$slice": ["$top_movies", N]}}}
        ]
    result = db.movies.aggregate(pipeline)
    return list(result)

#--------------------------------------------------------------------------------------------------------------------
