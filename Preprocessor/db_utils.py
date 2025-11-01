"""
Shared database utilities for Discord message harvesting and processing.

This module provides:
- Database connection management
- Efficient duplicate checking using processed_ids collection
- Safe insertion helpers
- Message tracking utilities
"""

from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
import time
from datetime import datetime
from typing import List, Dict, Optional

# MongoDB connection string


class DatabaseManager:
    """Manages MongoDB connections and operations for message processing."""

    def __init__(self):
        """Initialize database connection."""
        self.client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
        self.db = self.client["JobStats"]
        self.interview_collection = self.db["interview_processes"]
        self.processed_collection = self.db["processed_ids"]
        self.unprocessed_collection = self.db["unprocessed_messages"]
        self.archive_collection = self.db["archive"]

        # Ensure indexes exist
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create indexes for efficient lookups."""
        try:
            # Index on msg_id for processed_ids collection
            self.processed_collection.create_index([("msg_id", ASCENDING)], unique=True)
            # Index on msg_id for unprocessed_messages collection
            self.unprocessed_collection.create_index([("msg_id", ASCENDING)], unique=True)
            # Index on msg_id for archive collection
            self.archive_collection.create_index([("msg_id", ASCENDING)])
            # Index on channel for unprocessed_messages
            self.unprocessed_collection.create_index([("channel", ASCENDING)])
        except Exception as e:
            # Indexes might already exist
            pass

    def test_connection(self):
        """Test MongoDB connection."""
        try:
            self.client.admin.command('ping')
            print("✅ Connected to MongoDB!")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False

    def is_message_processed(self, msg_id: str) -> bool:
        """
        Check if a message ID has already been processed.

        This is much faster than querying the main interview_processes collection.

        Args:
            msg_id: Discord message ID

        Returns:
            True if message has been processed, False otherwise
        """
        return self.processed_collection.find_one({"msg_id": msg_id}) is not None

    def are_messages_processed(self, msg_ids: List[str]) -> Dict[str, bool]:
        """
        Check multiple message IDs at once for efficiency.

        Args:
            msg_ids: List of Discord message IDs

        Returns:
            Dictionary mapping msg_id -> is_processed
        """
        if not msg_ids:
            return {}

        # Query all at once
        processed = self.processed_collection.find(
            {"msg_id": {"$in": msg_ids}},
            {"msg_id": 1, "_id": 0}
        )

        processed_set = {doc["msg_id"] for doc in processed}
        return {msg_id: msg_id in processed_set for msg_id in msg_ids}

    def mark_message_processed(self, msg_id: str, spam: bool = False, source: str = "harvesting"):
        """
        Mark a single message as processed.

        Args:
            msg_id: Discord message ID
            spam: Whether this message was classified as spam
            source: Source of processing (e.g., 'harvesting', 'parsing')
        """
        try:
            self.processed_collection.insert_one({
                "msg_id": msg_id,
                "processed_at": datetime.utcnow().isoformat(),
                "spam": spam,
                "source": source
            })
        except Exception as e:
            # Duplicate key error is fine - message already processed
            if "duplicate key" not in str(e).lower():
                print(f"⚠️  Error marking message {msg_id} as processed: {e}")

    def mark_messages_processed(self, msg_ids: List[str], spam: bool = False, source: str = "harvesting"):
        """
        Mark multiple messages as processed (batch operation).

        Args:
            msg_ids: List of Discord message IDs
            spam: Whether these messages were classified as spam
            source: Source of processing
        """
        if not msg_ids:
            return

        docs = [{
            "msg_id": msg_id,
            "processed_at": datetime.utcnow().isoformat(),
            "spam": spam,
            "source": source
        } for msg_id in msg_ids]

        try:
            # ordered=False allows continuing even if some are duplicates
            self.processed_collection.insert_many(docs, ordered=False)
        except Exception as e:
            # Duplicate key errors are expected for already-processed messages
            if "duplicate key" not in str(e).lower():
                print(f"⚠️  Error marking messages as processed: {e}")

    def safe_insert_one(self, doc: Dict, retries: int = 3) -> Optional[object]:
        """
        Insert a single document into interview_processes with retry logic.

        Args:
            doc: Document to insert
            retries: Number of retry attempts

        Returns:
            Insert result or None if failed
        """
        for attempt in range(retries):
            try:
                result = self.interview_collection.insert_one(doc)
                print(f"✅ Inserted document for {doc.get('author')} - {doc.get('company')}")
                return result
            except Exception as e:
                print(f"⚠️  Insert failed (attempt {attempt+1}/{retries}): {e}")
                time.sleep(2)

        print("❌ Failed to insert document after retries.")
        return None

    def safe_insert_many(self, docs: List[Dict], retries: int = 3) -> Optional[object]:
        """
        Insert multiple documents into interview_processes with retry logic.

        Args:
            docs: List of documents to insert
            retries: Number of retry attempts

        Returns:
            Insert result or None if failed
        """
        if not docs:
            return None

        for attempt in range(retries):
            try:
                result = self.interview_collection.insert_many(docs, ordered=False)
                print(f"✅ Batch inserted {len(result.inserted_ids)} documents")
                return result
            except Exception as e:
                print(f"⚠️  Batch insert failed (attempt {attempt+1}/{retries}): {e}")
                time.sleep(2)

        print("❌ Failed to insert batch after retries.")
        return None

    def check_duplicate_entry(self, author: str, company: str, stage: str) -> bool:
        """
        Check if a specific entry already exists in interview_processes.

        This prevents duplicate entries for the same author/company/stage combination.

        Args:
            author: Discord username
            company: Company name
            stage: Interview stage

        Returns:
            True if duplicate exists, False otherwise
        """
        exists = self.interview_collection.find_one({
            "author": author,
            "company": company,
            "stage": stage
        })
        return exists is not None

    def add_unprocessed_messages(self, messages: List[Dict], channel: str) -> int:
        """
        Add messages to unprocessed_messages collection.

        Args:
            messages: List of Discord message documents
            channel: Channel identifier

        Returns:
            Number of messages successfully added
        """
        if not messages:
            return 0

        # Add channel field to each message
        for msg in messages:
            msg["channel"] = channel
            msg["harvested_at"] = datetime.utcnow().isoformat()

        try:
            # ordered=False allows continuing even if some are duplicates
            result = self.unprocessed_collection.insert_many(messages, ordered=False)
            count = len(result.inserted_ids)
            print(f"✅ Added {count} messages to unprocessed_messages")
            return count
        except Exception as e:
            # Handle duplicate key errors
            if "duplicate key" in str(e).lower():
                # Count successful inserts before error
                print(f"⚠️  Some messages already in unprocessed_messages")
                return 0
            else:
                print(f"❌ Error adding to unprocessed_messages: {e}")
                return 0

    def get_unprocessed_messages(self, channel: Optional[str] = None, limit: Optional[int] = None) -> List[Dict]:
        """
        Get messages from unprocessed_messages collection.

        Args:
            channel: Optional channel filter
            limit: Optional limit on number of messages

        Returns:
            List of unprocessed message documents
        """
        query = {}
        if channel:
            query["channel"] = channel

        cursor = self.unprocessed_collection.find(query)
        if limit:
            cursor = cursor.limit(limit)

        return list(cursor)

    def count_unprocessed_messages(self, channel: Optional[str] = None) -> int:
        """
        Count unprocessed messages.

        Args:
            channel: Optional channel filter

        Returns:
            Count of unprocessed messages
        """
        query = {}
        if channel:
            query["channel"] = channel

        return self.unprocessed_collection.count_documents(query)

    def archive_message(self, msg_id: str, spam: bool = False, classification: Optional[Dict] = None):
        """
        Move a message from unprocessed_messages to archive.

        Args:
            msg_id: Discord message ID
            spam: Whether message was classified as spam
            classification: Optional classification data
        """
        # Find message in unprocessed
        msg = self.unprocessed_collection.find_one({"msg_id": msg_id})
        if not msg:
            print(f"⚠️  Message {msg_id} not found in unprocessed_messages")
            return

        # Prepare archive document
        archive_doc = msg.copy()
        archive_doc["archived_at"] = datetime.utcnow().isoformat()
        archive_doc["spam"] = spam
        if classification:
            archive_doc["classification"] = classification

        # Insert into archive
        try:
            self.archive_collection.insert_one(archive_doc)
        except Exception as e:
            print(f"⚠️  Error archiving message {msg_id}: {e}")
            return

        # Remove from unprocessed
        self.unprocessed_collection.delete_one({"msg_id": msg_id})

    def archive_messages_batch(self, msg_ids: List[str], spam: bool = False):
        """
        Archive multiple messages at once.

        Args:
            msg_ids: List of Discord message IDs
            spam: Whether messages were classified as spam
        """
        if not msg_ids:
            return

        # Find messages in unprocessed
        messages = list(self.unprocessed_collection.find({"msg_id": {"$in": msg_ids}}))

        if not messages:
            return

        # Prepare archive documents
        archive_docs = []
        for msg in messages:
            archive_doc = msg.copy()
            archive_doc["archived_at"] = datetime.utcnow().isoformat()
            archive_doc["spam"] = spam
            archive_docs.append(archive_doc)

        # Insert into archive
        try:
            self.archive_collection.insert_many(archive_docs, ordered=False)
            print(f"✅ Archived {len(archive_docs)} messages")
        except Exception as e:
            print(f"⚠️  Error archiving messages: {e}")
            return

        # Remove from unprocessed
        self.unprocessed_collection.delete_many({"msg_id": {"$in": msg_ids}})

    def get_stats(self) -> Dict:
        """Get statistics about processed messages and interview data."""
        return {
            "total_processed": self.processed_collection.count_documents({}),
            "total_interviews": self.interview_collection.count_documents({}),
            "spam_messages": self.processed_collection.count_documents({"spam": True}),
            "non_spam_messages": self.processed_collection.count_documents({"spam": False}),
            "unprocessed_messages": self.unprocessed_collection.count_documents({}),
            "archived_messages": self.archive_collection.count_documents({}),
            "archived_spam": self.archive_collection.count_documents({"spam": True}),
            "archived_non_spam": self.archive_collection.count_documents({"spam": False})
        }

    def close(self):
        """Close database connection."""
        self.client.close()


# Singleton instance for easy importing
_db_manager = None


def get_db_manager() -> DatabaseManager:
    """Get or create the singleton DatabaseManager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager