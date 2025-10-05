"""
Database layer for Bank CRM API
Handles all CouchBase database operations and connections
"""

import os
import logging
import time
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.exceptions import DocumentNotFoundException, CouchbaseException, UnAmbiguousTimeoutException

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Database configuration class"""
    COUCHBASE_HOST = os.getenv("COUCHBASE_HOST", "couchbases://cb.5cy-u6ry6rixcogc.cloud.couchbase.com")
    COUCHBASE_USERNAME = os.getenv("COUCHBASE_USERNAME", "Shreya")
    COUCHBASE_PASSWORD = os.getenv("COUCHBASE_PASSWORD", "Shreya@12345")
    COUCHBASE_BUCKET = os.getenv("COUCHBASE_BUCKET", "customer_prospecting_bucket")
    COUCHBASE_SCOPE = os.getenv("COUCHBASE_SCOPE", "voice_bot_scope")
    COUCHBASE_COLLECTION = os.getenv("COUCHBASE_COLLECTION", "user_data")
    COUCHBASE_TICKETS_COLLECTION = os.getenv("COUCHBASE_TICKETS_COLLECTION", "tickets")
    CONNECTION_TIMEOUT = int(os.getenv("COUCHBASE_TIMEOUT", "30"))  # Reduced timeout
    MAX_RETRIES = int(os.getenv("COUCHBASE_MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("COUCHBASE_RETRY_DELAY", "2"))

class CouchBaseConnection:
    """
    CouchBase database connection and operations handler
    """
    
    def __init__(self):
        self.cluster = None
        self.bucket = None
        self.collection = None
        self.tickets_collection = None
        self.config = DatabaseConfig()
        self._connected = False
        
    def connect(self):
        """Establish connection to CouchBase"""
        try:
            logger.info(f"Connecting to CouchBase at {self.config.COUCHBASE_HOST}")
            
            # Initialize authentication
            auth = PasswordAuthenticator(
                self.config.COUCHBASE_USERNAME, 
                self.config.COUCHBASE_PASSWORD
            )
            
            # Create cluster connection
            self.cluster = Cluster(
                self.config.COUCHBASE_HOST, 
                ClusterOptions(auth)
            )
            
            # Wait until cluster is ready
            self.cluster.wait_until_ready(timedelta(seconds=self.config.CONNECTION_TIMEOUT))
            
            # Get bucket and collections
            self.bucket = self.cluster.bucket(self.config.COUCHBASE_BUCKET)
            self.collection = self.bucket.scope(self.config.COUCHBASE_SCOPE).collection(self.config.COUCHBASE_COLLECTION)
            self.tickets_collection = self.bucket.scope(self.config.COUCHBASE_SCOPE).collection(self.config.COUCHBASE_TICKETS_COLLECTION)
            
            self._connected = True
            logger.info("Successfully connected to CouchBase")
            
        except Exception as e:
            logger.error(f"Failed to connect to CouchBase: {e}")
            self._connected = False
            raise ConnectionError(f"Database connection failed: {str(e)}")

    def disconnect(self):
        """Close the database connection"""
        try:
            if self.cluster:
                self.cluster.disconnect()
                self._connected = False
                logger.info("Disconnected from CouchBase")
        except Exception as e:
            logger.error(f"Error disconnecting from CouchBase: {e}")

    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self._connected

    def get_customer_by_phone(self, phone_number: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve customer data by phone number
        
        Args:
            phone_number (str): Customer's phone number
            
        Returns:
            Optional[Dict[str, Any]]: Customer data or None if not found
            
        Raises:
            CouchbaseException: If database query fails
        """
        try:
            # Ensure connection is active
            self.ensure_connection()
            
            logger.info(f"Querying customer by phone: {phone_number}")
            
            query = f"""
            SELECT c.*
            FROM `{self.config.COUCHBASE_BUCKET}`.`{self.config.COUCHBASE_SCOPE}`.`{self.config.COUCHBASE_COLLECTION}` c
            WHERE c.personal_info.phone_number = $phone_number
            LIMIT 1
            """
            
            # Execute query with parameters
            result = self.cluster.query(
                query, 
                QueryOptions(named_parameters={"phone_number": phone_number})
            )
            
            # Process results
            rows = list(result)
            
            if rows:
                customer_data = rows[0]
                
                # Handle different result structures
                if 'customer_data' in customer_data:
                    return customer_data['customer_data']
                elif 'c' in customer_data:
                    return customer_data['c']
                else:
                    return customer_data
            
            logger.info(f"No customer found with phone: {phone_number}")
            return None

        except Exception as e:
            logger.error(f"Error querying customer by phone {phone_number}: {e}")
            raise CouchbaseException(f"Database query failed: {str(e)}")
    
    def get_customer_by_id(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve customer data by customer ID
        
        Args:
            customer_id (str): Customer's unique identifier
            
        Returns:
            Optional[Dict[str, Any]]: Customer data or None if not found
            
        Raises:
            CouchbaseException: If database operation fails
        """
        try:
            # Ensure connection is active
            self.ensure_connection()
            
            logger.info(f"Retrieving customer by ID: {customer_id}")
            
            result = self.collection.get(customer_id)
            return result.content_as[dict]
            
        except DocumentNotFoundException:
            logger.info(f"Customer not found with ID: {customer_id}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving customer {customer_id}: {e}")
            raise CouchbaseException(f"Database operation failed: {str(e)}")

    def get_customer_accounts(self, customer_id: str) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        """
        Get all accounts for a specific customer
        
        Args:
            customer_id (str): Customer's unique identifier
            
        Returns:
            Optional[Dict[str, List[Dict[str, Any]]]]: Customer account information
        """
        try:
            customer_data = self.get_customer_by_id(customer_id)
            
            if not customer_data:
                return None
            
            return {
                "banking_accounts": customer_data.get("banking_accounts", []),
                "credit_cards": customer_data.get("credit_cards", []),
                "loans": customer_data.get("loans", [])
            }
            
        except Exception as e:
            logger.error(f"Error retrieving accounts for customer {customer_id}: {e}")
            raise

    def get_customer_transactions(self, customer_id: str, limit: int = 10) -> Optional[Dict[str, Any]]:
        """
        Get recent transactions for a specific customer
        
        Args:
            customer_id (str): Customer's unique identifier
            limit (int): Maximum number of transactions to return
            
        Returns:
            Optional[Dict[str, Any]]: Transaction data
        """
        try:
            customer_data = self.get_customer_by_id(customer_id)
            
            if not customer_data:
                return None
            
            transactions = customer_data.get("recent_transactions", [])
            
            return {
                "transactions": transactions[:limit],
                "total_available": len(transactions)
            }
            
        except Exception as e:
            logger.error(f"Error retrieving transactions for customer {customer_id}: {e}")
            raise

    def search_customers(self, filters: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search customers with advanced filters
        
        Args:
            filters (Dict[str, Any]): Search filters
            limit (int): Maximum number of results to return
            
        Returns:
            List[Dict[str, Any]]: List of matching customers
        """
        try:
            # Build dynamic query based on filters
            where_clauses = []
            parameters = {}
            
            if "phone_number" in filters:
                where_clauses.append("c.personal_info.phone_number = $phone_number")
                parameters["phone_number"] = filters["phone_number"]
            
            if "email" in filters:
                where_clauses.append("c.personal_info.email = $email")
                parameters["email"] = filters["email"]
            
            if "customer_tier" in filters:
                where_clauses.append("c.account_info.customer_tier = $customer_tier")
                parameters["customer_tier"] = filters["customer_tier"]
            
            if not where_clauses:
                return []
            
            query = f"""
            SELECT c.*
            FROM `{self.config.COUCHBASE_BUCKET}`.`{self.config.COUCHBASE_SCOPE}`.`{self.config.COUCHBASE_COLLECTION}` c
            WHERE {' AND '.join(where_clauses)}
            LIMIT {limit}
            """
            
            result = self.cluster.query(
                query, 
                QueryOptions(named_parameters=parameters)
            )
            
            return [row.get('c', row) for row in result]
            
        except Exception as e:
            logger.error(f"Error searching customers: {e}")
            raise

    # Ticket-related methods
    def create_ticket(self, phone_number: str, issue: str, priority: str = "medium", category: str = "general") -> Dict[str, Any]:
        """
        Create a new support ticket
        
        Args:
            phone_number (str): Customer's phone number (user ID)
            issue (str): Issue description
            priority (str): Ticket priority
            category (str): Issue category
            
        Returns:
            Dict[str, Any]: Created ticket data
            
        Raises:
            CouchbaseException: If database operation fails
        """
        try:
            # Ensure connection is active
            self.ensure_connection()
            
            # Generate unique ticket ID
            ticket_id = f"TKT_{uuid.uuid4().hex[:8].upper()}_{int(time.time())}"
            
            # Create ticket data
            current_time = datetime.utcnow().isoformat()
            
            ticket_data = {
                "ticket_id": ticket_id,
                "phone_number": phone_number,
                "issue": issue,
                "priority": priority.lower(),
                "category": category.lower(),
                "status": "open",
                "created_at": current_time,
                "updated_at": current_time,
                "assigned_to": None,
                "resolution": None,
                "closed_at": None,
                "customer_satisfaction": None,
                "metadata": {
                    "source": "api",
                    "channel": "crm"
                }
            }
            
            # Insert into tickets collection
            self.tickets_collection.insert(ticket_id, ticket_data)
            
            logger.info(f"Created ticket {ticket_id} for phone {phone_number}")
            return ticket_data
            
        except Exception as e:
            logger.error(f"Error creating ticket for phone {phone_number}: {e}")
            raise CouchbaseException(f"Failed to create ticket: {str(e)}")
        

    def get_ticket_with_verification(self, ticket_id: str, last_four_digits: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve ticket with security verification of last 4 digits
        
        Args:
            ticket_id (str): Full ticket ID
            last_four_digits (str): Last 4 digits for verification
            
        Returns:
            Optional[Dict[str, Any]]: Ticket data if verification passes, None otherwise
        """
        try:
            # First verify the last 4 digits match
            if len(ticket_id) < 4 or ticket_id[-4:] != last_four_digits:
                logger.warning(f"Security verification failed for ticket: {ticket_id}")
                return None
            
            # If verification passes, retrieve the ticket
            return self.get_ticket_by_id(ticket_id)
            
        except Exception as e:
            logger.error(f"Error in ticket verification: {e}")
            return None

    def get_ticket_by_id(self, ticket_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve ticket by ticket ID
        
        Args:
            ticket_id (str): Ticket ID
            
        Returns:
            Optional[Dict[str, Any]]: Ticket data or None if not found
        """
        try:
            # Ensure connection is active
            self.ensure_connection()
            
            logger.info(f"Retrieving ticket: {ticket_id}")
            
            result = self.tickets_collection.get(ticket_id)
            return result.content_as[dict]
            
        except DocumentNotFoundException:
            logger.info(f"Ticket not found: {ticket_id}")
            return None
        except Exception as e:
            logger.error(f"Error retrieving ticket {ticket_id}: {e}")
            raise CouchbaseException(f"Database operation failed: {str(e)}")

    def get_tickets_by_phone(self, phone_number: str, status: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get all tickets for a specific phone number
        
        Args:
            phone_number (str): Customer's phone number
            status (Optional[str]): Filter by ticket status
            limit (int): Maximum number of tickets to return
            
        Returns:
            List[Dict[str, Any]]: List of tickets
        """
        try:
            # Ensure connection is active
            self.ensure_connection()
            
            logger.info(f"Retrieving tickets for phone: {phone_number}")
            
            # Build query
            where_clause = "t.phone_number = $phone_number"
            parameters = {"phone_number": phone_number}
            
            if status:
                where_clause += " AND t.status = $status"
                parameters["status"] = status.lower()
            
            query = f"""
            SELECT t.*
            FROM `{self.config.COUCHBASE_BUCKET}`.`{self.config.COUCHBASE_SCOPE}`.`{self.config.COUCHBASE_TICKETS_COLLECTION}` t
            WHERE {where_clause}
            ORDER BY t.created_at DESC
            LIMIT {limit}
            """
            
            result = self.cluster.query(
                query, 
                QueryOptions(named_parameters=parameters)
            )
            
            tickets = [row.get('t', row) for row in result]
            logger.info(f"Found {len(tickets)} tickets for phone: {phone_number}")
            return tickets
            
        except Exception as e:
            logger.error(f"Error retrieving tickets for phone {phone_number}: {e}")
            raise CouchbaseException(f"Database query failed: {str(e)}")

    def update_ticket_status(self, ticket_id: str, status: str, assigned_to: Optional[str] = None, resolution: Optional[str] = None) -> bool:
        """
        Update ticket status and other fields
        
        Args:
            ticket_id (str): Ticket ID
            status (str): New status
            assigned_to (Optional[str]): Assigned agent
            resolution (Optional[str]): Resolution description
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure connection is active
            self.ensure_connection()
            
            # Get current ticket data
            current_ticket = self.get_ticket_by_id(ticket_id)
            if not current_ticket:
                return False
            
            # Update fields
            current_ticket["status"] = status.lower()
            current_ticket["updated_at"] = datetime.utcnow().isoformat()
            
            if assigned_to:
                current_ticket["assigned_to"] = assigned_to
            
            if resolution:
                current_ticket["resolution"] = resolution
            
            if status.lower() in ["closed", "resolved"]:
                current_ticket["closed_at"] = datetime.utcnow().isoformat()
            
            # Replace document
            self.tickets_collection.replace(ticket_id, current_ticket)
            
            logger.info(f"Updated ticket {ticket_id} status to {status}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating ticket {ticket_id}: {e}")
            return False

    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics for health checks
        
        Returns:
            Dict[str, Any]: Database statistics
        """
        try:
            # Get customer count
            customer_query = f"""
            SELECT COUNT(*) as total_customers
            FROM `{self.config.COUCHBASE_BUCKET}`.`{self.config.COUCHBASE_SCOPE}`.`{self.config.COUCHBASE_COLLECTION}`
            """
            
            # Get ticket count
            ticket_query = f"""
            SELECT COUNT(*) as total_tickets
            FROM `{self.config.COUCHBASE_BUCKET}`.`{self.config.COUCHBASE_SCOPE}`.`{self.config.COUCHBASE_TICKETS_COLLECTION}`
            """
            
            customer_result = list(self.cluster.query(customer_query))
            ticket_result = list(self.cluster.query(ticket_query))
            
            return {
                "total_customers": customer_result[0]["total_customers"] if customer_result else 0,
                "total_tickets": ticket_result[0]["total_tickets"] if ticket_result else 0,
                "connection_status": "healthy" if self._connected else "disconnected",
                "bucket": self.config.COUCHBASE_BUCKET,
                "scope": self.config.COUCHBASE_SCOPE,
                "collections": {
                    "customers": self.config.COUCHBASE_COLLECTION,
                    "tickets": self.config.COUCHBASE_TICKETS_COLLECTION
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {
                "total_customers": 0,
                "total_tickets": 0,
                "connection_status": "error",
                "error": str(e)
            }

    def test_connection(self) -> bool:
        """
        Test database connection
        
        Returns:
            bool: True if connection is working, False otherwise
        """
        try:
            if not self._connected:
                return False
                
            # Test with a simple query
            query = f"SELECT 1 as test FROM `{self.config.COUCHBASE_BUCKET}` LIMIT 1"
            result = list(self.cluster.query(query))
            return len(result) >= 0  # Even empty result means connection works
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            self._connected = False
            return False

    def ensure_connection(self):
        """Ensure database connection is active, reconnect if needed"""
        if not self.is_connected() or not self.test_connection():
            logger.info("Connection lost, attempting to reconnect...")
            self.connect()

# Utility functions for data operations
class DataUtilities:
    """Utility functions for data manipulation"""
    
    @staticmethod
    def mask_sensitive_data(customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mask sensitive information in customer data
        
        Args:
            customer_data (Dict[str, Any]): Raw customer data
            
        Returns:
            Dict[str, Any]: Customer data with masked sensitive information
        """
        if not customer_data:
            return customer_data
            
        masked_data = customer_data.copy()
        
        # Mask SSN
        if "personal_info" in masked_data and "ssn_last_4" in masked_data["personal_info"]:
            ssn = masked_data["personal_info"]["ssn_last_4"]
            if len(ssn) >= 4:
                masked_data["personal_info"]["ssn_last_4"] = f"***{ssn[-4:]}"
            else:
                masked_data["personal_info"]["ssn_last_4"] = "****"
        
        # Mask account numbers
        if "banking_accounts" in masked_data:
            for account in masked_data["banking_accounts"]:
                if "account_number" in account and len(account["account_number"]) >= 4:
                    account["account_number"] = f"****{account['account_number'][-4:]}"
        
        # Mask card numbers
        if "credit_cards" in masked_data:
            for card in masked_data["credit_cards"]:
                if "card_number" in card and len(card["card_number"]) >= 4:
                    card["card_number"] = f"****{card['card_number'][-4:]}"
        
        return masked_data   
    
    @staticmethod
    def filter_customer_data(customer_data: Dict[str, Any], include_transactions: bool = True, 
                           include_support_history: bool = False, include_account_summary: bool = True) -> Dict[str, Any]:
        """
        Filter customer data based on request parameters
        
        Args:
            customer_data (Dict[str, Any]): Raw customer data
            include_transactions (bool): Include recent transactions
            include_support_history (bool): Include support history
            include_account_summary (bool): Include account balance summary
            
        Returns:
            Dict[str, Any]: Filtered customer data
        """
        filtered_data = customer_data.copy()
        
        if not include_transactions:
            filtered_data.pop("recent_transactions", None)
        
        if not include_support_history:
            filtered_data.pop("support_history", None)
        
        if not include_account_summary:
            # Remove detailed account info but keep basic info
            if "banking_accounts" in filtered_data:
                for account in filtered_data["banking_accounts"]:
                    account.pop("balance", None)
        
        return filtered_data

# Database instance (singleton pattern)
_db_instance = None

def get_database() -> CouchBaseConnection:
    """
    Get database connection instance (singleton)
    
    Returns:
        CouchBaseConnection: Database connection instance
    """
    global _db_instance
    
    if _db_instance is None:
        _db_instance = CouchBaseConnection()
        
    if not _db_instance.is_connected():
        _db_instance.connect()
        
    return _db_instance

def close_database():
    """Close database connection"""
    global _db_instance
    
    if _db_instance:
        _db_instance.disconnect()
        _db_instance = None