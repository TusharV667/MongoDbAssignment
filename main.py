from db import connect_to_mongodb
from bulkLoad import bulk_load_collections
from functions import insert_comment, insert_movie, insert_theater, insert_user
from commentOperations import top_10_commenters, top_10_commented_movies, comments_per_month_in_year
from movieOperations import top_n_movies_highest_imdb_rating, print_top_n_movies_highest_imdb_rating_in_year, top_n_directors_most_movies_in_genre, top_n_actors_most_movies
from movieOperations import print_top_n_movies_highest_imdb_rating_votes, top_n_movies_title_matching_pattern_sorted_by_tomatoes, top_n_directors_most_movies
from movieOperations import top_n_actors_most_movies_in_year, top_n_actors_most_movies_in_genre, top_n_directors_most_movies_in_year, top_n_movies_by_genre_with_highest_imdb
from theaterOperations import top_10_cities_with_max_theaters, top_10_theaters_nearby

#--------------------------------------------------------------------------------------------------------------------
# You can uncomment the commented codes below. 
# First section is for the bulk loading of data.
# And the second section provides template for insertion of data in each of the collections.
#--------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    db = connect_to_mongodb()
    # bulk_load_collections(db)

#--------------------------------------------------------------------------------------------------------------------
# Raw data for insertion
       
    # new_comment = {"name":"Tushar Vyas",
    #                "email":"Tusharr@fakegmail.com",
    #                "movie_id":{"$oid":"573a1390f29313caabcd4323"},
    #                "text":"Awesome .",
    #                "date":{"$date":{"$numberLong":"187243230"}}}
    # new_movie = {"title": "New Movie", 
    #              "genre": "Action", 
    #              "year": 2024}
    # new_theater = {"name": "New Theater", 
    #                "location": "New York"}
    # new_user = {"username": "newuser", 
    #             "email": "newuser@example.com"}

# --------------------------------------------------------------------------------------------------------------------
# Insert new documents into respective collections
    
    # insert_comment(db, new_comment)
    # insert_movie(db, new_movie)
    # insert_theater(db, new_theater)
    # insert_user(db, new_user)

#--------------------------------------------------------------------------------------------------------------------
    top_commentors = top_10_commenters(db)
    print("Top 10 users who made the maximum number of comments:")
    for commentor in top_commentors:
        print(commentor)

#--------------------------------------------------------------------------------------------------------------------
    print("\nTop 10 movies with most comments:")
    top_movies = top_10_commented_movies(db)
    for  movie in top_movies:
        print(movie)

#--------------------------------------------------------------------------------------------------------------------
    N = 10
    year = 1998
    print(f"\nTotal number of comments created each month in the year {year}:")
    data = comments_per_month_in_year(db, 1998)  # Update with your desired year  

    for entry in data:
        year = entry["_id"]["year"]
        month = entry["_id"]["month"]
        total_comments = entry["total_comments"]
        print(f"In {year}-{month:02}, {total_comments} comments were created.")

#--------------------------------------------------------------------------------------------------------------------

    print(f"\nTop {N} IMDB rated movies are:")
    top_rated_movies = top_n_movies_highest_imdb_rating(N, db)
    for movie in top_rated_movies:
        print("Title: "+movie["title"]+", Rating: "+str(movie["imdb"]["rating"]))

#--------------------------------------------------------------------------------------------------------------------
    print(f"\nTop {N} IMDB rated movies in {year} are:")
    top_rated_movies_of_year = print_top_n_movies_highest_imdb_rating_in_year(N, year, db)
    for movie in top_rated_movies_of_year:
        print("Title: "+movie["title"]+", Rating: "+str(movie["imdb"]["rating"]))

#--------------------------------------------------------------------------------------------------------------------
    print(f"\nTop {N} movies with the highest IMDB rating and number of votes > 1000")
    movies_vote_1000 = print_top_n_movies_highest_imdb_rating_votes(N, db)
    for movie in movies_vote_1000:
        print("Title: "+movie["title"]+", Rating: "+str(movie["imdb"]["rating"]))

#--------------------------------------------------------------------------------------------------------------------
    pattern = "action"
    print(f"\nTop {N} movies matching the pattern '{pattern}'")
    top_movies_matching_pattern_sorted_by_tomatoes = top_n_movies_title_matching_pattern_sorted_by_tomatoes(N, pattern, db)
    for movie in top_movies_matching_pattern_sorted_by_tomatoes:
        print("Title: "+movie["title"] +", Rating: "+str(movie["tomatoes"]["viewer"]["rating"]))

#--------------------------------------------------------------------------------------------------------------------
    print(f"\nTop {N} director with most movies:")
    top_directors_most_movies = top_n_directors_most_movies(N, db)
    for directors in top_directors_most_movies:
        print(directors)

#--------------------------------------------------------------------------------------------------------------------
    print(f"\nTop {N} director with most movies in year {year}:")
    top_directors_most_movies_in_year = top_n_directors_most_movies_in_year(N, year, db)
    for directors in top_directors_most_movies_in_year:
            print(directors)

#--------------------------------------------------------------------------------------------------------------------
    genre = "Comedy"
    print(f"\nTop {N} director with most movies in genre {genre}:")
    top_directors_most_movies_in_genre = top_n_directors_most_movies_in_genre(N, genre, db)
    for directors in top_directors_most_movies_in_genre:
            print(directors)

#--------------------------------------------------------------------------------------------------------------------
    print(f"\nTop {N} actors with most movies:")
    top_actors_most_movies = top_n_actors_most_movies(N, db)
    for actor in top_actors_most_movies:
        print(actor["_id"], actor["count"])

#--------------------------------------------------------------------------------------------------------------------
    print(f"\nTop {N} actors with most movies in the year {year}:")
    top_actors_most_movies_in_year = top_n_actors_most_movies_in_year(N, year, db)
    for actor in top_actors_most_movies_in_year:
        print(actor["_id"], actor["count"])

#--------------------------------------------------------------------------------------------------------------------
    genre = "Comedy"
    print(f"\nTop {N} actors with most movies in genre {genre}:")
    top_actors_most_movies_in_genre = top_n_actors_most_movies_in_genre(N, genre, db)
    for actor in top_actors_most_movies_in_genre:
        print(actor["_id"], actor["count"])

#--------------------------------------------------------------------------------------------------------------------
    N = 1
    print(f"\nTop {N} movies for each genre with highest IMDB ratings:")
    top_movies_by_genre_with_highest_imdb = top_n_movies_by_genre_with_highest_imdb(N, db)
    for genre_movies in top_movies_by_genre_with_highest_imdb:
        print("Genre:", genre_movies["genre"])
        for movie in genre_movies["top_movies"]:
            print(movie)

#--------------------------------------------------------------------------------------------------------------------
    print(f"\nTop 10 cities with max number of theaters:")
    top_10_cities = top_10_cities_with_max_theaters(db)
    for city in top_10_cities:
        print("City:", city["_id"], "- Number of Theaters:", city["count"])

#--------------------------------------------------------------------------------------------------------------------
    coordinates = [-96.608055, 33.685692]
    print(f"\nTop 10 theaters nearby the given coordinates are:")
    unique_cities = top_10_theaters_nearby(coordinates, db)
    for city in unique_cities:
        print(city)

#--------------------------------------------------------------------------------------------------------------------
