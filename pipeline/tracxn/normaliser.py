"""
Normalise raw Tracxn API responses into internal data models.

The Tracxn API returns nested JSON with varying field names depending
on the endpoint.  This module maps that into our canonical Company /
Founder / FundingRound models so downstream code never touches raw JSON.
"""

from __future__ import annotations

import re
import logging
from typing import Any

from pipeline.models.schema import (
    Company,
    DataSource,
    Education,
    Founder,
    FundingRound,
    WorkExperience,
)

log = logging.getLogger(__name__)


def _slug(text: str) -> str:
    """Convert a string to a URL-safe slug."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


# ------------------------------------------------------------------
# Company normalisation
# ------------------------------------------------------------------

def normalise_company(raw: dict[str, Any]) -> Company:
    """Map a raw Tracxn company dict to our Company model."""
    name = raw.get("name") or raw.get("companyName") or "Unknown"
    founded = raw.get("yearFounded") or raw.get("foundedYear")

    # Funding rounds
    rounds: list[FundingRound] = []
    for r in raw.get("fundingRounds", raw.get("rounds", [])):
        rounds.append(
            FundingRound(
                round_type=r.get("roundType", r.get("type", "unknown")),
                amount_usd=r.get("amount", r.get("amountUSD")),
                date=r.get("date", r.get("announcedDate")),
                investors=[
                    inv.get("name", inv) if isinstance(inv, dict) else inv
                    for inv in r.get("investors", [])
                ],
            )
        )

    # Founder references (just names at this stage)
    founder_names: list[str] = []
    for p in raw.get("people", raw.get("founders", [])):
        if isinstance(p, dict):
            pname = p.get("name", p.get("fullName", ""))
        else:
            pname = str(p)
        if pname:
            founder_names.append(pname)

    return Company(
        id=_slug(name),
        name=name,
        domain=raw.get("domain") or raw.get("website"),
        sector=raw.get("sector", raw.get("primarySector", "")),
        sub_sector=raw.get("subSector"),
        city=raw.get("city") or raw.get("hqCity"),
        country=raw.get("country") or raw.get("hqCountry", "India"),
        founded_year=int(founded) if founded else None,
        total_funding_usd=raw.get("totalFundingAmountUSD")
        or raw.get("totalFunding")
        or raw.get("totalFundingAmount"),
        valuation_usd=raw.get("valuation") or raw.get("latestValuation"),
        employee_count=raw.get("employeeCount"),
        founders=[_slug(n) for n in founder_names],
        funding_rounds=rounds,
        source=DataSource.TRACXN,
        tracxn_id=raw.get("id") or raw.get("tracxnId"),
    )


# ------------------------------------------------------------------
# Founder normalisation
# ------------------------------------------------------------------

def normalise_founder(raw: dict[str, Any], company_slug: str = "") -> Founder:
    """Map a raw Tracxn person/founder dict to our Founder model."""
    name = raw.get("name") or raw.get("fullName") or "Unknown"

    # Education
    education: list[Education] = []
    for ed in raw.get("education", []):
        if isinstance(ed, dict):
            education.append(
                Education(
                    institution=ed.get("institution", ed.get("school", "")),
                    degree=ed.get("degree"),
                    field=ed.get("field", ed.get("fieldOfStudy")),
                    year=ed.get("year", ed.get("endYear")),
                )
            )
        elif isinstance(ed, str):
            education.append(Education(institution=ed))

    # Work history
    work: list[WorkExperience] = []
    for w in raw.get("workExperience", raw.get("experience", [])):
        if isinstance(w, dict):
            work.append(
                WorkExperience(
                    company=w.get("company", w.get("organization", "")),
                    role=w.get("role", w.get("title")),
                    start_year=w.get("startYear"),
                    end_year=w.get("endYear"),
                )
            )
        elif isinstance(w, str):
            work.append(WorkExperience(company=w))

    companies = [company_slug] if company_slug else []

    return Founder(
        id=_slug(f"{name}-{company_slug}" if company_slug else name),
        name=name,
        companies=companies,
        education=education,
        work_history=work,
        linkedin_url=raw.get("linkedinUrl") or raw.get("linkedin"),
        source=DataSource.TRACXN,
        verified=False,
    )


# ------------------------------------------------------------------
# Batch normalisation
# ------------------------------------------------------------------

def normalise_company_batch(raw_list: list[dict]) -> list[Company]:
    """Normalise a list of raw company dicts."""
    results = []
    for raw in raw_list:
        try:
            results.append(normalise_company(raw))
        except Exception:
            log.warning("Failed to normalise company: %s", raw.get("name", "?"))
    return results
