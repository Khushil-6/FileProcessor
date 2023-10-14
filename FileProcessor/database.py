# database.py
from pymongo import MongoClient
from FileProcessor.config import MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION


class MongoDB:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DATABASE]
        self.collection = self.db[MONGODB_COLLECTION]

    def store_metadata(self, metadata):
        self.collection.insert_one(metadata)

    def metadata_exists(self, file_path):
        return self.collection.find_one({"file_path": file_path})
