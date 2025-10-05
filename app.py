"""
Bank CRM API - Main Application
Customer Relationship Management API for Bank Customer Service
"""

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
from starlette.responses import JSONResponse

# Import our database layer
from database import get_database, close_database, DataUtilities, CouchBaseConnection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app initialization
app = FastAPI(
    title="Bank CRM API",
    description="Customer Relationship Management API for Bank Customer Service",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class CustomerResponse(BaseModel):
    customer_id: str
    personal_info: Dict[str, Any]
    account_info: Dict[str, Any]
    banking_accounts: List[Dict[str, Any]]
    credit_cards: List[Dict[str, Any]]
    loans: List[Dict[str, Any]]
    recent_transactions: List[Dict[str, Any]]
    alerts_preferences: Dict[str, Any]
    security_info: Dict[str, Any]
    support_history: List[Dict[str, Any]]
    metadata: Dict[str, Any]

class TicketCreateRequest(BaseModel):
    phone_number: str = Field(..., description="Customer phone number (user ID)")
    issue: str = Field(..., min_length=10, max_length=1000, description="Issue description")
    priority: Optional[str] = Field(default="medium", description="Ticket priority: low, medium, high, urgent")
    category: Optional[str] = Field(default="general", description="Issue category")

class TicketResponse(BaseModel):
    ticket_id: str
    phone_number: str
    issue: str
    priority: str
    category: str
    status: str
    created_at: str
    updated_at: str
    assigned_to: Optional[str] = None

class ErrorResponse(BaseModel):
    error: str
    message: str
    timestamp: str

class CustomerSearchRequest(BaseModel):
    phone_number: str = Field(..., description="Customer phone number")
    include_transactions: bool = Field(default=True, description="Include recent transactions")
    include_support_history: bool = Field(default=False, description="Include support ticket history")

class CustomerSearchFilters(BaseModel):
    phone_number: Optional[str] = None
    email: Optional[str] = None
    customer_tier: Optional[str] = None
    limit: int = Field(default=10, le=100, description="Maximum number of results")

# Dependency to get database connection
def get_db() -> CouchBaseConnection:
    """Dependency to get database connection"""
    try:
        return get_database()
    except ConnectionError as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(
            status_code=503,
            detail="Database service temporarily unavailable. Please try again later."
        )

# API Routes
@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "service": "Bank CRM API",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }

@app.get("/health", tags=["Health"])
async def health_check(db: CouchBaseConnection = Depends(get_db)):
    """Detailed health check including database connectivity"""
    try:
        # Test database connection and get stats
        is_connected = db.test_connection()
        db_stats = db.get_database_stats()
        
        return {
            "status": "healthy" if is_connected else "unhealthy",
            "database": db_stats,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "database": {
                "connection_status": "error",
                "error": str(e)
            },
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/api/v1/customers/lookup", tags=["Customers"])
async def lookup_customer_by_phone(
    phone_number: str,
    include_account_summary: bool = True,
    include_transactions: bool = True,
    include_support_history: bool = False,
    db: CouchBaseConnection = Depends(get_db)
):
    """
    Lookup customer by phone number
    
    - **phone_number**: Customer's phone number (required)
    - **include_account_summary**: Include account balance summary (default: true)
    - **include_transactions**: Include recent transactions (default: true)
    - **include_support_history**: Include support ticket history (default: false)
    """
    try:
        logger.info(f"Looking up customer by phone: {phone_number}")
        
        # Clean phone number
        
        # Retrieve customer from database
        customer_data = db.get_customer_by_phone(phone_number)
        
        if not customer_data:
            raise HTTPException(
                status_code=404,
                detail="Customer not found with the provided phone number"
            )
        
        # Filter data based on request parameters
        filtered_data = DataUtilities.filter_customer_data(
            customer_data,
            include_transactions=include_transactions,
            include_support_history=include_support_history,
            include_account_summary=include_account_summary
        )
        
        # Mask sensitive data
        masked_data = DataUtilities.mask_sensitive_data(filtered_data)
        
        logger.info(f"Successfully retrieved customer: {customer_data.get('customer_id', 'Unknown')}")
        return masked_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving customer: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while retrieving customer data"
        )

@app.get("/api/v1/customers/{customer_id}", response_model=CustomerResponse, tags=["Customers"])
async def get_customer_by_id(
    customer_id: str,
    db: CouchBaseConnection = Depends(get_db)
):
    """
    Get customer details by customer ID
    
    - **customer_id**: Unique customer identifier
    """
    try:
        logger.info(f"Looking up customer with ID: {customer_id}")
        
        # Retrieve customer from database
        customer_data = db.get_customer_by_id(customer_id)
        
        if not customer_data:
            raise HTTPException(
                status_code=404,
                detail="Customer not found with the provided ID"
            )
        
        # Mask sensitive data
        masked_data = DataUtilities.mask_sensitive_data(customer_data)
        
        logger.info(f"Successfully retrieved customer: {customer_id}")
        return masked_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving customer {customer_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while retrieving customer data"
        )

@app.post("/api/v1/customers/search", response_model=List[CustomerResponse], tags=["Customers"])
async def search_customers(
    search_request: CustomerSearchRequest,
    db: CouchBaseConnection = Depends(get_db)
):
    """
    Search customers with advanced filters
    """
    try:
        customer_data = db.get_customer_by_phone(search_request.phone_number)
        
        if not customer_data:
            return []
        
        # Apply filters based on request
        filtered_data = DataUtilities.filter_customer_data(
            customer_data,
            include_transactions=search_request.include_transactions,
            include_support_history=search_request.include_support_history
        )
        
        masked_data = DataUtilities.mask_sensitive_data(filtered_data)
        return [masked_data]
        
    except Exception as e:
        logger.error(f"Error searching customers: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while searching customers"
        )

@app.post("/api/v1/customers/advanced-search", response_model=List[CustomerResponse], tags=["Customers"])
async def advanced_search_customers(
    filters: CustomerSearchFilters,
    db: CouchBaseConnection = Depends(get_db)
):
    """
    Advanced search customers with multiple filter options
    """
    try:
        # Convert filters to dictionary
        search_filters = {}
        
        if filters.phone_number:
            search_filters["phone_number"] = filters.phone_number
        if filters.email:
            search_filters["email"] = filters.email
        if filters.customer_tier:
            search_filters["customer_tier"] = filters.customer_tier
        
        # Search customers
        customers = db.search_customers(search_filters, filters.limit)
        
        # Process results
        results = []
        for customer_data in customers:
            masked_data = DataUtilities.mask_sensitive_data(customer_data)
            results.append(masked_data)
        
        return results
        
    except Exception as e:
        logger.error(f"Error in advanced search: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while searching customers"
        )

@app.get("/api/v1/customers/{customer_id}/accounts", tags=["Accounts"])
async def get_customer_accounts(
    customer_id: str,
    db: CouchBaseConnection = Depends(get_db)
):
    """Get all accounts for a specific customer"""
    try:
        accounts_data = db.get_customer_accounts(customer_id)
        
        if accounts_data is None:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        return {
            "customer_id": customer_id,
            **accounts_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving accounts for customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/customers/{customer_id}/transactions", tags=["Transactions"])
async def get_customer_transactions(
    customer_id: str,
    limit: int = 10,
    db: CouchBaseConnection = Depends(get_db)
):
    """Get recent transactions for a specific customer"""
    try:
        transactions_data = db.get_customer_transactions(customer_id, limit)
        
        if transactions_data is None:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        return {
            "customer_id": customer_id,
            **transactions_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving transactions for customer {customer_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/customers/{customer_id}/summary", tags=["Customers"])
async def get_customer_summary(
    customer_id: str,
    db: CouchBaseConnection = Depends(get_db)
):
    """Get a condensed summary of customer information"""
    try:
        customer_data = db.get_customer_by_id(customer_id)
        
        if not customer_data:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        # Create summary
        summary = {
            "customer_id": customer_data.get("customer_id", ""),
            "name": customer_data.get("personal_info", {}).get("full_name", ""),
            "phone": customer_data.get("personal_info", {}).get("phone_number", ""),
            "email": customer_data.get("personal_info", {}).get("email", ""),
            "customer_tier": customer_data.get("account_info", {}).get("customer_tier", ""),
            "status": customer_data.get("account_info", {}).get("status", ""),
            "total_accounts": len(customer_data.get("banking_accounts", [])),
            "total_cards": len(customer_data.get("credit_cards", [])),
            "total_loans": len(customer_data.get("loans", [])),
            "last_login": customer_data.get("account_info", {}).get("last_login", "")
        }
        
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating customer summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Ticket endpoints
@app.get("/api/v1/tickets/search", tags=["Tickets"])
async def search_tickets_by_partial_id(
    partial_id: str = Query(..., min_length=4, description="Partial ticket ID (at least 4 characters)"),
    phone_number: str = Query(..., description="Customer phone number for verification"),
    db: CouchBaseConnection = Depends(get_db)
):
    """
    Search for tickets using partial ticket ID and phone number verification
    
    - **partial_id**: Partial ticket ID (minimum 4 characters)
    - **phone_number**: Customer's phone number for security verification
    """
    try:
        logger.info(f"Searching tickets by partial ID for phone: {phone_number}")
        
        # Clean phone number
        
        # Get customer tickets
        tickets = db.get_tickets_by_phone(phone_number, limit=50)
        
        # Filter by partial ID match
        matching_tickets = []
        partial_id_upper = partial_id.upper()
        
        for ticket in tickets:
            ticket_id = ticket.get("ticket_id", "").upper()
            if partial_id_upper in ticket_id:
                # Add security information
                ticket["security_info"] = {
                    "last_four_digits": ticket_id[-4:] if len(ticket_id) >= 4 else "N/A",
                    "verification_required": True
                }
                matching_tickets.append(ticket)
        
        # Limit results for security
        limited_results = matching_tickets[:5]
        
        logger.info(f"Found {len(limited_results)} matching tickets")
        return {
            "matches_found": len(limited_results),
            "total_customer_tickets": len(tickets),
            "tickets": limited_results,
            "security_note": "To retrieve full ticket details, provide the complete ticket ID and last 4 digits for verification"
        }
        
    except Exception as e:
        logger.error(f"Error searching tickets by partial ID: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while searching tickets"
        )

@app.post("/api/v1/tickets", response_model=TicketResponse, tags=["Tickets"])
async def create_ticket(
    ticket_request: TicketCreateRequest,
    db: CouchBaseConnection = Depends(get_db)
):
    """
    Create a new support ticket
    
    - **phone_number**: Customer's phone number (user ID)
    - **issue**: Detailed description of the issue (10-1000 characters)
    - **priority**: Ticket priority (low, medium, high, urgent) - default: medium
    - **category**: Issue category - default: general
    """
    try:
        logger.info(f"Creating ticket for phone: {ticket_request.phone_number}")
        
        # Clean phone number
        
        # Verify customer exists (optional validation)
        customer_data = db.get_customer_by_phone(ticket_request.phone_number)
        if not customer_data:
            logger.warning(f"Creating ticket for non-existing customer: {ticket_request.phone_number}")
        
        # Create ticket
        ticket_data = db.create_ticket(
            phone_number=ticket_request.phone_number,
            issue=ticket_request.issue,
            priority=ticket_request.priority,
            category=ticket_request.category
        )
        
        logger.info(f"Successfully created ticket: {ticket_data['ticket_id']}")
        return ticket_data
        
    except Exception as e:
        logger.error(f"Error creating ticket: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while creating ticket"
        )

@app.get("/api/v1/tickets/{ticket_id}", response_model=TicketResponse, tags=["Tickets"])
async def get_ticket(
    ticket_id: str,
    db: CouchBaseConnection = Depends(get_db)
):
    """Get ticket details by ticket ID"""
    try:
        logger.info(f"Retrieving ticket: {ticket_id}")
        
        ticket_data = db.get_ticket_by_id(ticket_id)
        
        if not ticket_data:
            raise HTTPException(
                status_code=404,
                detail="Ticket not found with the provided ID"
            )
        
        logger.info(f"Successfully retrieved ticket: {ticket_id}")
        return ticket_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving ticket {ticket_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while retrieving ticket"
        )

@app.get("/api/v1/customers/{phone_number}/tickets", response_model=List[TicketResponse], tags=["Tickets"])
async def get_customer_tickets(
    phone_number: str,
    status: Optional[str] = None,
    limit: int = Query(default=10, le=50, description="Maximum number of tickets to return"),
    db: CouchBaseConnection = Depends(get_db)
):
    """Get all tickets for a specific customer by phone number"""
    try:
        logger.info(f"Retrieving tickets for phone: {phone_number}")
        
        # Clean phone number
        
        tickets = db.get_tickets_by_phone(phone_number, status, limit)
        
        logger.info(f"Retrieved {len(tickets)} tickets for phone: {phone_number}")
        return tickets
        
    except Exception as e:
        logger.error(f"Error retrieving tickets for phone {phone_number}: {e}")
        raise HTTPException(
            status_code=500,
            detail="Internal server error while retrieving customer tickets"
        )

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "message": str(exc.detail),
            "timestamp": datetime.utcnow().isoformat()
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error", 
            "message": "An unexpected error occurred",
            "timestamp": datetime.utcnow().isoformat()
        }
    )

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("Starting Bank CRM API service")
    # Don't fail startup if database is not available
    # Let individual requests handle connection issues
    logger.info("Database connection will be established on first request")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down Bank CRM API service")
    try:
        close_database()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )