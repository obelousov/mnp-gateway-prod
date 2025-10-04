from fastapi import APIRouter, HTTPException, status, Depends
from typing import List
from models.schemas import (
    MsisdnQueryRequest, 
    MsisdnQueryResponse,
    QueryStatus
)
from services.mnp_service import MnpService

router = APIRouter()

@router.post(
    "/query-msisdn", 
    response_model=MsisdnQueryResponse,
    summary="Query MSISDN portability details",
    description="Accept MSISDN query from BSS, store in DB, and forward to Central Node"
)
async def query_msisdn_details(
    request: MsisdnQueryRequest,
    mnp_service: MnpService = Depends()
):