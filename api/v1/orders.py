from fastapi import FastAPI, HTTPException, Depends, APIRouter
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
import json
from services.database_service import get_db_connection


# Pydantic models
# class PortabilityQuery(BaseModel):
#     id: Optional[int] = None
#     msisdn: Optional[str] = None
#     contract_number: Optional[str] = None
#     request_type: Optional[str] = None
#     response_status: Optional[str] = None
#     document_number: Optional[str] = None
#     created_start: Optional[datetime] = None
#     created_end: Optional[datetime] = None

class PortabilityQuery(BaseModel):
    id: Optional[int] = Field(
        None,
        description="Unique identifier of the portability request",
        example=189
    )
    msisdn: Optional[str] = Field(
        None,
        description="MSISDN (phone number)",
        example="621800000",
        max_length=15
    )
    contract_number: Optional[str] = Field(
        None,
        description="Subscriber contract number with the operator. Initial 3 digits must be equal to operator code",
        example="299-TRAC_12",
        max_length=100
    )
    request_type: Optional[str] = Field(
        None,
        description="Type of portability request. Possible values: PORT_IN, PORT_OUT, CANCELLATION",
        example="PORT_IN",
        enum=["PORT_IN", "PORT_OUT", "CANCELLATION"]
    )
    response_status: Optional[str] = Field(
        None,
        description="Latest response status code from NC",
        example="ASOL",
        max_length=10
    )
    document_number: Optional[str] = Field(
        None,
        description="Official identification document number (NIF, CIF, NIE, PAS)",
        example="Y3037876D",
        max_length=50
    )
    reference_code: Optional[str] = Field(  # Added this field
        None,
        description="Unique reference code from NC",
        example="29979811251030102500001",
        max_length=100
    )
    created_start: Optional[datetime] = Field(
        None,
        description="Start date for filtering requests created after this timestamp (inclusive)",
        example="2025-10-28T10:25:03"
    )
    created_end: Optional[datetime] = Field(
        None,
        description="End date for filtering requests created before this timestamp (inclusive)",
        example="2025-10-30T13:50:57"
    )

    class Config:
        schema_extra = {
            "example": {
                "id": 189,
                "request_type": "PORT_IN",
                "reference_code": "29979811251030102500001",
                "response_status": "ASOL",
                "msisdn": "621800000",
                "document_number": "Y3037876D",
                "contract_number": "299-TRAC_12",
                "created_start": "2025-10-28T10:25:03",
                "created_end": "2025-10-30T13:50:57"
            }
        }
class PortabilityResponse(BaseModel):
    id: int
    country_code: str
    request_type: str
    reference_code: Optional[str]
    session_code: Optional[str]
    status_bss: Optional[str]
    status_nc: Optional[str]
    response_code: Optional[str]
    response_status: Optional[str]
    description: Optional[str]
    msisdn: str
    document_type: str
    document_number: str
    name_surname: str
    contract_number: Optional[str]
    donor_operator: str
    recipient_operator: str
    desired_porting_date: Optional[str]
    requested_at: Optional[datetime]
    scheduled_at: Optional[datetime]
    completed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

class SearchResponse(BaseModel):
    total_records: int
    data: List[PortabilityResponse]


# FastAPI app
# app = FastAPI(title="Portability Requests API")
router = APIRouter()

@router.post(
    "/orders-search",
    response_model=SearchResponse,
    summary="Search Portability Requests",
    description="""
    Search portability requests using flexible filtering criteria.
    
    **Key Features:**
    - Search by any combination of parameters
    - All parameters are optional
    - Returns results matching ALL provided criteria (AND logic)
    - Results are ordered by creation date (newest first)
    
    **Common Use Cases:**
    - Find all PORT_OUT requests for a specific MSISDN
    - Search requests by contract number and document
    - Filter by date range and response status
    - Retrieve requests by specific ID
    """,
    response_description="List of portability requests matching the search criteria",
    tags=["Spain: Portability Operations"]
)
async def search_portability_requests(
    query: PortabilityQuery,
    limit: int = 100,
    offset: int = 0
):
    """
    Search portability requests with detailed filtering options.
    
    Args:
        query: Search criteria including:
            - id: Unique request identifier
            - msisdn: Phone number (15 digits max)
            - contract_number: Subscriber contract number
            - request_type: PORT_IN, PORT_OUT, CANCELLATION, MULTISIM, or EXTENSION
            - response_status: NC system response code
            - document_number: Identification document number
            - created_start: Start date for creation range
            - created_end: End date for creation range
        limit: Maximum number of results to return (default: 100, max: 1000)
        offset: Number of results to skip for pagination (default: 0)
    
    Returns:
        List of portability requests matching the criteria
    
    Raises:
        HTTPException: 500 - Database connection or query error
    
    Examples:
        - Search by MSISDN and request type
        - Filter by date range and response status
        - Find requests by contract number
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)

        # Build base query without LIMIT for count
        count_query = """
        SELECT COUNT(*) as total
        FROM portability_requests 
        WHERE 1=1
        """
        
        data_query = """
        SELECT 
            id, country_code, request_type, reference_code, session_code,
            status_bss, status_nc, response_code, response_status, description,
            msisdn, document_type, document_number, name_surname, contract_number,
            donor_operator, recipient_operator, desired_porting_date,
            requested_at, scheduled_at, completed_at, created_at, updated_at
        FROM portability_requests 
        WHERE 1=1
        """

        params = []
        
        # Build WHERE clause for both queries
        if query.id:
            count_query += " AND id = %s"
            data_query += " AND id = %s"
            params.append(query.id)
        
        if query.msisdn:
            count_query += " AND msisdn = %s"
            data_query += " AND msisdn = %s"
            params.append(query.msisdn)
        
        if query.contract_number:
            count_query += " AND contract_number = %s"
            data_query += " AND contract_number = %s"
            params.append(query.contract_number)
        
        if query.request_type:
            count_query += " AND request_type = %s"
            data_query += " AND request_type = %s"
            params.append(query.request_type)
        
        if query.response_status:
            count_query += " AND response_status = %s"
            data_query += " AND response_status = %s"
            params.append(query.response_status)
        
        if query.document_number:
            count_query += " AND document_number = %s"
            data_query += " AND document_number = %s"
            params.append(query.document_number)
        
        if query.reference_code:
            count_query += " AND reference_code = %s"
            data_query += " AND reference_code = %s"
            params.append(query.reference_code)
        
        if query.created_start:
            count_query += " AND created_at >= %s"
            data_query += " AND created_at >= %s"
            params.append(query.created_start)
        
        if query.created_end:
            count_query += " AND created_at <= %s"
            data_query += " AND created_at <= %s"
            params.append(query.created_end)

        if query.id:
            count_query += " AND id = %s"
            data_query += " AND id = %s"
            params.append(query.id)


        # First, get total count
        cursor.execute(count_query, params)
        count_result = cursor.fetchone()
        total_records = count_result['total']
        
        # Then, get the data with ordering
        data_query += " ORDER BY created_at DESC"
        cursor.execute(data_query, params)
        results = cursor.fetchall()
        
        # Convert datetime objects
        for result in results:
            for key, value in result.items():
                if isinstance(value, datetime):
                    result[key] = value.isoformat()
        
        return {
            "total_records": total_records,
            "data": results
        }
            
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@router.get("/portability-requests/{request_id}", 
            response_model=PortabilityResponse,
            include_in_schema=False)  # This hides the endpoint from Swagger))
async def get_portability_request(request_id: int):
    """
    Get specific portability request by ID
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        query = """
        SELECT 
            id, country_code, request_type, reference_code, session_code,
            status_bss, status_nc, response_code, response_status, description,
            msisdn, document_type, document_number, name_surname, contract_number,
            donor_operator, recipient_operator, desired_porting_date,
            requested_at, scheduled_at, completed_at, created_at, updated_at
        FROM portability_requests 
        WHERE id = %s
        """
        
        cursor.execute(query, (request_id,))
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Portability request not found")
        
        # Convert datetime objects
        for key, value in result.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
        
        return result
        
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

@router.get("/portability-requests",
            include_in_schema=False)
async def get_portability_requests(
    msisdn: Optional[str] = None,
    contract_number: Optional[str] = None,
    request_type: Optional[str] = None,
    response_status: Optional[str] = None,
    document_number: Optional[str] = None,
    created_start: Optional[datetime] = None,
    created_end: Optional[datetime] = None
):
    """
    Alternative endpoint using query parameters instead of JSON body
    """
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)

        sql_query = """
        SELECT 
            id, country_code, request_type, reference_code, session_code,
            status_bss, status_nc, response_code, response_status, description,
            msisdn, document_type, document_number, name_surname, contract_number,
            donor_operator, recipient_operator, desired_porting_date,
            requested_at, scheduled_at, completed_at, created_at, updated_at
        FROM portability_requests 
        WHERE 1=1
        """
        
        params = []
        
        if msisdn:
            sql_query += " AND msisdn = %s"
            params.append(msisdn)
        
        if contract_number:
            sql_query += " AND contract_number = %s"
            params.append(contract_number)
        
        if request_type:
            sql_query += " AND request_type = %s"
            params.append(request_type)
        
        if response_status:
            sql_query += " AND response_status = %s"
            params.append(response_status)
        
        if document_number:
            sql_query += " AND document_number = %s"
            params.append(document_number)
        
        if created_start:
            sql_query += " AND created_at >= %s"
            params.append(created_start)
        
        if created_end:
            sql_query += " AND created_at <= %s"
            params.append(created_end)
        
        sql_query += " ORDER BY created_at DESC"
        
        cursor.execute(sql_query, params)
        results = cursor.fetchall()
        
        # Convert datetime objects
        for result in results:
            for key, value in result.items():
                if isinstance(value, datetime):
                    result[key] = value.isoformat()
        
        return results
        
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

# Health check endpoint
@router.get("/health-db",
            tags=["Spain: Portability Operations"])
async def health_check():
    connection = None
    try:
        connection = get_db_connection()
        if connection.is_connected():
            return {"status": "healthy", "database": "connected", "timestamp": datetime.now().isoformat()}
        else:
            return {"status": "unhealthy", "database": "disconnected", "timestamp": datetime.now().isoformat()}
    except Error as e:
        return {"status": "unhealthy", "error": str(e), "timestamp": datetime.now().isoformat()}
    finally:
        if connection and connection.is_connected():
            connection.close()