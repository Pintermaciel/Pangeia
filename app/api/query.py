from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.analysis_service import execute_analysis

router = APIRouter()


class QueryRequest(BaseModel):
    question: str


@router.post("/execute_query/")
async def execute_query(request: QueryRequest):
    try:
        result = execute_analysis(request.question)
        return {"result": result}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
            )
