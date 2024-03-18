# Function to insert a new document into the comments collection
def insert_comment(db, comment_data):
    db["comments"].insert_one(comment_data)
    print("New comment inserted successfully!")

# Function to insert a new document into the movies collection
def insert_movie(db, movie_data):
    db['movies'].insert_one(movie_data)
    print("New movie inserted successfully!")

# Function to insert a new document into the theaters collection
def insert_theater(db, theater_data):
    db['theaters'].insert_one(theater_data)
    print("New theater inserted successfully!")

# Function to insert a new document into the users collection
def insert_user(db, user_data):
    db['users'].insert_one(user_data)
    print("New user inserted successfully!")
