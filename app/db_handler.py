
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from datetime import datetime
import logging
from typing import List, Dict, Any
import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_operations.log'),
        logging.StreamHandler()
    ]
)

class MongoDBHandler:
    def __init__(self):
        self.mongo_uri = os.getenv('MONGODB_URI')
        self.db_name = os.getenv('DB_NAME', 'clicflyer_db')
        self.collection_name = os.getenv('COLLECTION_NAME', 'offers')
        self.client = None
        self.db = None
        self.collection = None

    def connect(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logging.info("Successfully connected to MongoDB")
        except PyMongoError as e:
            logging.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logging.info("MongoDB connection closed")

    def update_offers(self, offers: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Update offers in MongoDB using bulk operations
        Returns statistics about the operation
        """
        if not self.client:
            self.connect()

        try:
            operations = []
            timestamp = datetime.utcnow()

            for offer in offers:
                offer['last_updated'] = timestamp
                
                offer_id = str(offer.get('Id'))
                retailer_id = str(offer.get('Retailer ID'))
                unique_id = f"{retailer_id}_{offer_id}"
                operations.append(
                    UpdateOne(
                        {'_id': unique_id},
                        {
                            '$set': offer,
                            '$setOnInsert': {'first_seen': timestamp}
                        },
                        upsert=True
                    )
                )
            if operations:
                result = self.collection.bulk_write(operations)
                
                stats = {
                    'matched': result.matched_count,
                    'modified': result.modified_count,
                    'upserted': result.upserted_count
                }
                
                logging.info(f"Database update stats: {stats}")
                return stats
            
            return {'matched': 0, 'modified': 0, 'upserted': 0}

        except PyMongoError as e:
            logging.error(f"Failed to update offers in MongoDB: {str(e)}")
            raise
        finally:
            self.close()

    def get_latest_offers(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve the most recently updated offers"""
        if not self.client:
            self.connect()

        try:
            latest_offers = list(
                self.collection
                .find({}, {'_id': 0})
                .sort('last_updated', -1)
                .limit(limit)
            )
            return latest_offers
        except PyMongoError as e:
            logging.error(f"Failed to retrieve latest offers: {str(e)}")
            raise
        finally:
            self.close()

    # def cleanup_old_offers(self, days: int = 30) -> int:
    #     """Remove offers older than specified days"""
    #     if not self.client:
    #         self.connect()

    #     try:
    #         cutoff_date = datetime.utcnow() - timedelta(days=days)
    #         result = self.collection.delete_many({
    #             'last_updated': {'$lt': cutoff_date}
    #         })
    #         logging.info(f"Removed {result.deleted_count} old offers")
    #         return result.deleted_count
    #     except PyMongoError as e:
    #         logging.error(f"Failed to cleanup old offers: {str(e)}")
    #         raise
    #     finally:
    #         self.close()