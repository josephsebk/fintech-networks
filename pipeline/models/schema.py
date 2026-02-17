"""
Core data models for companies, founders, and network relationships.

Uses Pydantic for validation — these models are the canonical internal
representation that every stage of the pipeline reads/writes.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ------------------------------------------------------------------
# Enums
# ------------------------------------------------------------------

class DataSource(str, Enum):
    TRACXN = "tracxn"
    MANUAL = "manual"
    CRUNCHBASE = "crunchbase"
    LINKEDIN = "linkedin"


class RelationshipType(str, Enum):
    CO_FOUNDER = "co_founder"
    SAME_COLLEGE = "same_college"
    SAME_EMPLOYER = "same_employer"
    SAME_BATCH = "same_batch"
    INVESTOR_FOUNDER = "investor_founder"
    MENTOR = "mentor"


# ------------------------------------------------------------------
# Core models
# ------------------------------------------------------------------

class Education(BaseModel):
    institution: str
    degree: Optional[str] = None
    field: Optional[str] = None
    year: Optional[int] = None


class WorkExperience(BaseModel):
    company: str
    role: Optional[str] = None
    start_year: Optional[int] = None
    end_year: Optional[int] = None


class Founder(BaseModel):
    id: str = Field(description="Unique slug: lowercase-name-company")
    name: str
    companies: list[str] = Field(default_factory=list)
    education: list[Education] = Field(default_factory=list)
    work_history: list[WorkExperience] = Field(default_factory=list)
    linkedin_url: Optional[str] = None
    source: DataSource = DataSource.MANUAL
    verified: bool = False
    tags: list[str] = Field(default_factory=list)


class FundingRound(BaseModel):
    round_type: str  # seed, series_a, etc.
    amount_usd: Optional[int] = None
    date: Optional[str] = None
    investors: list[str] = Field(default_factory=list)


class Company(BaseModel):
    id: str = Field(description="Unique slug: lowercase-name")
    name: str
    domain: Optional[str] = None
    sector: str = ""
    sub_sector: Optional[str] = None
    city: Optional[str] = None
    country: str = "India"
    founded_year: Optional[int] = None
    total_funding_usd: Optional[int] = None
    valuation_usd: Optional[int] = None
    employee_count: Optional[int] = None
    founders: list[str] = Field(default_factory=list, description="Founder IDs")
    funding_rounds: list[FundingRound] = Field(default_factory=list)
    source: DataSource = DataSource.MANUAL
    tracxn_id: Optional[str] = None


class NetworkEdge(BaseModel):
    """An edge in the founder network graph."""
    source_id: str
    target_id: str
    relationship: RelationshipType
    weight: float = 1.0
    context: Optional[str] = None  # e.g. "IIT Delhi 2004"


class NetworkSnapshot(BaseModel):
    """A point-in-time snapshot of the entire dataset."""
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    sector: str
    companies: list[Company] = Field(default_factory=list)
    founders: list[Founder] = Field(default_factory=list)
    edges: list[NetworkEdge] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)
