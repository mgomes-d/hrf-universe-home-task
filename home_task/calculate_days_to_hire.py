from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import delete
from sqlalchemy import and_

import uuid

import numpy as np
from models import JobPosting, Statistics
from collections import defaultdict

DATABASE = "postgresql+psycopg2://admin:adm1n_password@localhost/home_task"

def get_row_stmt(job_id, jobs, country):
    values = [job.days_to_hire for job in jobs]
    values.sort()
    p10 = np.percentile(values, 10)
    p90 = np.percentile(values, 90)
    filtered = [value for value in values if p10 <= value <= p90]
    insert_stmt = Statistics(
        id=str(uuid.uuid4()),
        country_code=str(country),
        standard_job_id=str(job_id),
        avg_days="{:.1f}".format(np.average(filtered)),
        min_days="{:.1f}".format(p10),
        max_days="{:.1f}".format(p90),
        job_postings_number=int(len(jobs))
    )
    return insert_stmt
def main(min_job_postings_threshold: bool=True):
    engine = create_engine(DATABASE)

    Session = sessionmaker(bind=engine)

    country_values_stmt = select(JobPosting).where(
        and_(
            JobPosting.days_to_hire.isnot(None),
            JobPosting.country_code.isnot(None)
        )
    )
    all_values_stmt = select(JobPosting).where(
        and_(
            JobPosting.days_to_hire.isnot(None),
        )
    )
    with Session() as session:
        ## For each country and standard job create a separate row in a table.
        session.execute(delete(Statistics))
        country_values = session.execute(country_values_stmt).scalars().all()
        grouped_values = defaultdict(lambda: defaultdict(list))
        for job in country_values:
            grouped_values[job.country_code][job.standard_job_id].append(job)
        for country in grouped_values:
            ## group by country and jobs
            for job_id, jobs in grouped_values[country].items():
                if min_job_postings_threshold is True and len(jobs) < 5:
                    continue
                session.add(get_row_stmt(
                    job_id=job_id,
                    jobs=jobs,
                    country=country
                ))

        ## world per standard job || country value in a world will be None
        all_values = session.execute(all_values_stmt).scalars().all()
        world_values = defaultdict(list)
        for job in all_values:
            world_values[job.standard_job_id].append(job)
        
        for job_id, jobs in world_values.items():
            if min_job_postings_threshold is True and len(jobs) < 5:
                    continue
            session.add(get_row_stmt(
                    job_id=job_id,
                    jobs=jobs,
                    country=None
                ))

        session.commit()

if __name__ == "__main__":
    main()