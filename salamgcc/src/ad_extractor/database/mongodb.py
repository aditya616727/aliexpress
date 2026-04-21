"""MongoDB Atlas client for storing scraped listings."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from loguru import logger
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure, DuplicateKeyError

from ..config import settings


class MongoDBClient:
    """MongoDB Atlas client for scraper data."""

    def __init__(self) -> None:
        self.uri = settings.mongodb_uri
        self.client: Optional[MongoClient] = None
        self.db: Any = None

    def connect(self) -> bool:
        """Connect to MongoDB."""
        try:
            if not self.uri:
                raise ValueError("MONGODB_URI not found in environment variables")

            self.client = MongoClient(
                self.uri,
                tls=True,
                tlsAllowInvalidCertificates=True,
                serverSelectionTimeoutMS=5000,
            )
            self.client.admin.command("ping")
            self.db = self.client.get_default_database()
            logger.info(f"Connected to MongoDB: {self.db.name}")
            self._create_indexes()
            return True

        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            return False

    def _create_indexes(self) -> None:
        """Create indexes for better query performance."""
        try:
            listings = self.db.listings
            listings.create_index([("source_url", ASCENDING)], unique=True)
            listings.create_index([("business_id", ASCENDING)])
            listings.create_index([("source_site", ASCENDING)])
            listings.create_index([("scraped_at", DESCENDING)])
            listings.create_index([
                ("business_id", ASCENDING),
                ("source_site", ASCENDING),
                ("scraped_at", DESCENDING),
            ])

            brokers = self.db.brokers
            # Historical cleanup:
            # A previously-created UNIQUE index on brokers.business_id causes inserts to fail
            # when broker docs don't have business_id (Mongo treats missing as null, so only
            # one document is allowed). We no longer rely on business_id for brokers, so we
            # drop that legacy unique index if present.
            try:
                existing = brokers.index_information()
                idx = existing.get("business_id_1")
                if idx and idx.get("unique"):
                    brokers.drop_index("business_id_1")
            except Exception as e:
                logger.warning(f"Failed to drop legacy brokers business_id index: {e}")

            brokers.create_index([("source_site", ASCENDING), ("source_url", ASCENDING)], unique=True)
            brokers.create_index([("email", ASCENDING)])
            brokers.create_index([("name", ASCENDING)])
            brokers.create_index([("agencyName", ASCENDING)])
            brokers.create_index([("scraped_at", DESCENDING)])
            logger.info("MongoDB indexes created")
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")

    def insert_listing(self, listing_data: Dict[str, Any]) -> Optional[Any]:
        """Insert a single listing into MongoDB. Returns inserted_id or None."""
        try:
            if not hasattr(self, "db") or self.db is None:
                logger.error("MongoDB not connected! Call connect() first.")
                return None

            listing_data["scraped_at"] = datetime.utcnow()
            result = self.db.listings.insert_one(listing_data)
            title = listing_data.get("postAdData", {}).get("title", "Unknown")
            logger.info(f"Inserted listing: {title} (ID: {result.inserted_id})")

            verify = self.db.listings.find_one({"_id": result.inserted_id})
            if verify is None:
                logger.error(f"Verification failed: Document {result.inserted_id} not found")
                return None

            return result.inserted_id

        except DuplicateKeyError:
            logger.warning(f"Listing already exists: {listing_data.get('source_url')}")
            return None
        except Exception as e:
            logger.error(f"Failed to insert listing: {e}")
            return None

    def insert_many_listings(self, listings: List[Dict[str, Any]]) -> int:
        """Bulk insert listings. Returns count of inserted documents."""
        try:
            if not listings:
                return 0
            for listing in listings:
                listing["scraped_at"] = datetime.utcnow()
            result = self.db.listings.insert_many(listings, ordered=False)
            # logger.info(f"Bulk inserted {len(result.inserted_ids)} listings")
            return len(result.inserted_ids)
        except BulkWriteError as e:
            inserted = (e.details or {}).get("nInserted", (e.details or {}).get("insertedCount", 0))
            logger.warning(f"Bulk insert partial: {inserted} inserted, some duplicates skipped")
            return inserted
        except Exception as e:
            logger.error(f"Failed to bulk insert: {e}")
            return 0

    def upsert_broker(self, broker_data: Dict[str, Any]) -> bool:
        """
        Upsert a broker document into MongoDB.

        Expects keys:
        - source_site (str)
        - source_url (str) unique within source_site
        Plus broker schema fields like name, email, phone, profileImage, agencyName, etc.
        """
        try:
            if not hasattr(self, "db") or self.db is None:
                logger.error("MongoDB not connected! Call connect() first.")
                return False

            source_site = broker_data.get("source_site")
            source_url = broker_data.get("source_url")
            if not source_site or not source_url:
                logger.error("Broker upsert requires source_site and source_url")
                return False

            now = datetime.utcnow()
            broker_data = dict(broker_data)
            broker_data["updated_at"] = now

            result = self.db.brokers.update_one(
                {"source_site": source_site, "source_url": source_url},
                {
                    "$set": broker_data,
                    "$setOnInsert": {"created_at": now, "scraped_at": now},
                },
                upsert=True,
            )
            if result.upserted_id:
                logger.info(f"Inserted broker: {broker_data.get('name', 'Unknown')} ({source_site})")
            else:
                logger.info(f"Updated broker: {broker_data.get('name', 'Unknown')} ({source_site})")
            return True
        except DuplicateKeyError:
            logger.warning(
                f"Broker already exists: {broker_data.get('source_site')} {broker_data.get('source_url')}"
            )
            return False
        except Exception as e:
            logger.error(f"Failed to upsert broker: {e}")
            return False

    def update_listing(self, source_url: str, update_data: Dict[str, Any]) -> bool:
        """Update an existing listing by source_url."""
        try:
            update_data["updated_at"] = datetime.utcnow()
            result = self.db.listings.update_one(
                {"source_url": source_url},
                {"$set": update_data},
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to update listing: {e}")
            return False

    def get_existing_source_urls(self, business_id: str) -> set:
        """Get set of source_urls already in DB for a business (for skip-already-scraped)."""
        try:
            cursor = self.db.listings.find(
                {"business_id": business_id},
                {"source_url": 1, "_id": 0},
            )
            return {doc["source_url"] for doc in cursor if doc.get("source_url")}
        except Exception as e:
            logger.error(f"Failed to get existing URLs: {e}")
            return set()

    def get_listing_by_url(self, source_url: str) -> Optional[Dict[str, Any]]:
        """Get a listing by its source URL."""
        try:
            return self.db.listings.find_one({"source_url": source_url})
        except Exception as e:
            logger.error(f"Failed to get listing: {e}")
            return None

    def get_listings_by_business(self, business_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all listings for a business."""
        try:
            return list(
                self.db.listings.find({"business_id": business_id})
                .sort("scraped_at", DESCENDING)
                .limit(limit)
            )
        except Exception as e:
            logger.error(f"Failed to get listings: {e}")
            return []

    def count_listings(self, business_id: Optional[str] = None) -> int:
        """Count total listings or by business."""
        try:
            if business_id:
                return self.db.listings.count_documents({"business_id": business_id})
            return self.db.listings.count_documents({})
        except Exception as e:
            logger.error(f"Failed to count listings: {e}")
            return 0

    def delete_listing(self, source_url: str) -> bool:
        """Delete a listing by URL."""
        try:
            result = self.db.listings.delete_one({"source_url": source_url})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Failed to delete listing: {e}")
            return False

    def close(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


# Global MongoDB client instance
mongo_client = MongoDBClient()
