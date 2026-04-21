"""Database module - MongoDB client for storing scraped listings."""

from .mongodb import MongoDBClient, mongo_client

__all__ = ["MongoDBClient", "mongo_client"]
