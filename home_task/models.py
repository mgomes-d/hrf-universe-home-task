from dataclasses import dataclass
from typing import Optional

from sqlalchemy import (
    Column,
    Integer,
    String,
    Table,
    Float
)
from sqlalchemy.orm import registry

mapper_registry = registry()


class Model:
    pass


@mapper_registry.mapped
@dataclass
class StandardJobFamily(Model):
    __table__ = Table(
        "standard_job_family",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("name", String, nullable=False),
        schema="public",
    )

    id: str
    name: str


@mapper_registry.mapped
@dataclass
class StandardJob(Model):
    __table__ = Table(
        "standard_job",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("name", String, nullable=False),
        Column("standard_job_family_id", String, nullable=False),
        schema="public",
    )

    id: str
    name: str
    standard_job_family_id: str


@mapper_registry.mapped
@dataclass
class JobPosting(Model):
    __table__ = Table(
        "job_posting",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("title", String, nullable=False),
        Column("standard_job_id", String, nullable=False),
        Column("country_code", String, nullable=True),
        Column("days_to_hire", Integer, nullable=True),
        schema="public",
    )

    id: str
    title: str
    standard_job_id: str
    country_code: Optional[str] = None
    days_to_hire: Optional[int] = None

@mapper_registry.mapped
@dataclass
class Statistics(Model):
    __table__ = Table(
        "statistics",
        mapper_registry.metadata,
        Column("id", String, nullable=False, primary_key=True),
        Column("standard_job_id", String, nullable=False),
        Column("country_code", String, nullable=True),
        Column("avg_days", Float, nullable=False),
        Column("min_days", Float, nullable=False),
        Column("max_days", Float, nullable=False),
        Column("job_postings_number", Integer, nullable=False),
        schema="public",
    )
    id: str
    standard_job_id: str
    country_code: Optional[str]
    avg_days: float
    min_days: float
    max_days: float
    job_postings_number: int

