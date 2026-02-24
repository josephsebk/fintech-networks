"""
Normalise raw Tracxn Playground API responses into internal data models.

The Playground API returns nested JSON with field paths like:
  - totalEquityFunding.amount.USD.value
  - hqCity, hqCountry
  - foundedYear (int)
  - domain (list of strings)
  - companySectors (list of sector objects)

This module maps that into our canonical Company / Founder / FundingRound
models so downstream code never touches raw JSON.
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


def _deep_get(d: dict, path: str, default: Any = None) -> Any:
    """Safely traverse nested dicts via dot-separated path."""
    keys = path.split(".")
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key, default)
        else:
            return default
    return d


# ------------------------------------------------------------------
# Company normalisation
# ------------------------------------------------------------------

def normalise_company(raw: dict[str, Any]) -> Company:
    """Map a raw Tracxn Playground company dict to our Company model."""
    name = raw.get("name") or raw.get("companyName") or "Unknown"
    founded = raw.get("foundedYear") or raw.get("yearFounded")

    # Funding — Playground nests as totalEquityFunding.amount.USD.value
    total_funding = (
        _deep_get(raw, "totalEquityFunding.amount.USD.value")
        or _deep_get(raw, "totalFunding.amount.USD.value")
        or raw.get("totalFundingAmountUSD")
        or raw.get("totalFunding")
        or raw.get("totalFundingAmount")
    )

    # Valuation
    valuation = (
        _deep_get(raw, "latestValuation.amount.USD.value")
        or raw.get("valuation")
        or raw.get("latestValuation")
    )

    # Sector — Playground uses companySectors list
    sector = ""
    sectors_list = raw.get("companySectors", [])
    if sectors_list and isinstance(sectors_list, list):
        first = sectors_list[0]
        if isinstance(first, dict):
            sector = first.get("name", first.get("sectorName", ""))
        elif isinstance(first, str):
            sector = first
    if not sector:
        sector = raw.get("sector", raw.get("primarySector", ""))

    # Domain — may be string or list
    domain = raw.get("domain") or raw.get("website")
    if isinstance(domain, list):
        domain = domain[0] if domain else None

    # City / country
    city = raw.get("hqCity") or raw.get("city")
    country = raw.get("hqCountry") or raw.get("country", "India")

    # Funding rounds
    rounds: list[FundingRound] = []
    for r in raw.get("fundingRounds", raw.get("rounds", [])):
        amount = (
            _deep_get(r, "amount.USD.value")
            or r.get("amount")
            or r.get("amountUSD")
        )
        rounds.append(
            FundingRound(
                round_type=r.get("roundType", r.get("type", "unknown")),
                amount_usd=int(amount) if amount else None,
                date=r.get("date") or r.get("announcedDate"),
                investors=[
                    inv.get("name", inv) if isinstance(inv, dict) else str(inv)
                    for inv in r.get("investors", [])
                ],
            )
        )

    # Founder / people references
    founder_names: list[str] = []
    for p in raw.get("people", raw.get("founders", raw.get("keyPeople", []))):
        if isinstance(p, dict):
            pname = p.get("name") or p.get("fullName", "")
        else:
            pname = str(p)
        if pname:
            founder_names.append(pname)

    return Company(
        id=_slug(name),
        name=name,
        domain=domain,
        sector=sector,
        sub_sector=raw.get("subSector"),
        city=city,
        country=country,
        founded_year=int(founded) if founded else None,
        total_funding_usd=int(total_funding) if total_funding else None,
        valuation_usd=int(valuation) if valuation else None,
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
