# MongoDB Assignment

## Setup

1. **Clone the Repository**: 
    ```
    git clone https://github.com/TusharV667/MongoDbAssignment.git
    ```
2. **Install Dependencies**:
    ```
    pip install -r requirements.txt
    ```
3. **MongoDB Configuration**:
    - Make sure MongoDB is installed and running locally, or specify the MongoDB connection URI in `db.py`.

## Usage

- **Bulk Loading Data**:
    - Uncomment the function call for `bulkload.py` in `main.py` to bulk load JSON files into respective MongoDB collections.

- **Performing Operations**:
    - Use `main.py` to perform various operations on the collections.

## Folder Structure

- **Jsons/**: Contains JSON files for data to be loaded into MongoDB collections.
- **main.py**: Main entry point for the application.
- **db.py**: Contains code for connecting to MongoDB.
- **bulkload.py**: Handles bulk loading of JSON files into MongoDB.
- **function.py**: Contains common functions for inserting in different collections operations.
- **theaterOperations.py**, **moviesOperations.py**, **commentsOperations.py**: Contains methods and MongoDB queries for each collection.

## Additional Notes

- Ensure your MongoDB instance is properly configured and accessible.
- Just run the main.py. It will give the output for all th questions asked in assignments.

## Contributors

- Tushar Vyas

