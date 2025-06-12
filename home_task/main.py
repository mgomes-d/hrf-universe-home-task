from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

from sqlalchemy import select

from home_task.db import get_session
from home_task.models import Statistics
import logging

app = FastAPI()
logger = logging.getLogger(__name__)

class StatisticsResponse(BaseModel):
    standard_job_id: str
    country_code: Optional[str]
    min_days: float
    avg_days: float
    max_days: float
    job_postings_number: int

@app.get("/statistics")
async def get_statistics(
    standard_job_id: str = Query(...),
    country_code: Optional[str] = Query(None)
):
    try:
        with get_session() as session:
            statement = select(Statistics).where(
                Statistics.standard_job_id == standard_job_id
            )
            if country_code:
                statement = statement.where(Statistics.country_code == country_code)
            else:
                statement = statement.where(Statistics.country_code.is_(None))

            result = session.execute(statement).scalar_one_or_none()
            if result is None:
                raise HTTPException(status_code = 404, detail="Statistics not found")
            
            return StatisticsResponse(
                standard_job_id=result.standard_job_id,
                country_code=result.country_code,
                min_days=result.min_days,
                avg_days=result.avg_days,
                max_days=result.max_days,
                job_postings_number=result.job_postings_number,
            )

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise HTTPException(status_code=500, detail=e)