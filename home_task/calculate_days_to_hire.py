from sqlalchemy import select, delete, and_
from sqlalchemy.orm import selectinload
import uuid
from db import get_session
import numpy as np
from models import JobPosting, Statistics
from itertools import groupby
from operator import attrgetter
import argparse


def get_row_stmt(job_id, jobs, country):
    values = [job.days_to_hire for job in jobs]
    values.sort()
    p10 = np.percentile(values, 10)
    p90 = np.percentile(values, 90)
    filtered = [value for value in values if p10 <= value <= p90]
    return Statistics(
        id=str(uuid.uuid4()),
        country_code=country,
        standard_job_id=job_id,
        avg_days=round(float(np.average(filtered)), 1),
        min_days=round(float(p10), 1),
        max_days=round(float(p90), 1),
        job_postings_number=len(jobs),
    )


def main(min_job_postings_threshold: int = 5):
    with get_session() as session:
        try:
            session.execute(delete(Statistics))

            # 1. Country-level stats
            country_query = (
                session.query(
                    JobPosting.country_code,
                    JobPosting.standard_job_id,
                    JobPosting.days_to_hire
                )
                .filter(
                    JobPosting.days_to_hire.isnot(None),
                    JobPosting.country_code.isnot(None)
                )
                .order_by(JobPosting.country_code, JobPosting.standard_job_id)
                .execution_options(stream_results=True)
                .yield_per(1000)
            )

            key_fn = attrgetter("country_code", "standard_job_id")

            for (country, job_id), group in groupby(country_query, key=key_fn):
                jobs = list(group)
                if len(jobs) < min_job_postings_threshold:
                    continue
                session.add(get_row_stmt(job_id, jobs, country))

            # 2. World-level stats
            world_query = (
                session.query(
                    JobPosting.standard_job_id,
                    JobPosting.days_to_hire
                )
                .filter(JobPosting.days_to_hire.isnot(None))
                .order_by(JobPosting.standard_job_id)
                .execution_options(stream_results=True)
                .yield_per(1000)
            )

            world_key_fn = attrgetter("standard_job_id")

            for job_id, group in groupby(world_query, key=world_key_fn):
                jobs = list(group)
                if len(jobs) < min_job_postings_threshold:
                    continue
                session.add(get_row_stmt(job_id, jobs, country=None))

            session.commit()

        except Exception:
            session.rollback()
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=int, default=5)
    args = parser.parse_args()
    main(min_job_postings_threshold=args.threshold)