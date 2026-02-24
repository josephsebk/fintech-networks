"""
Data fetcher — orchestrates pulling data from Tracxn and writing
normalised JSON to disk.

This is the "E" and "T" in a lightweight ETL pipeline:
  1. Read a sector config
  2. Pull companies from Tracxn matching the config filters
  3. For each company, pull founder / people detail
  4. Normalise everything into canonical models
  5. Write JSON snapshots to data/raw/ and data/processed/
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline.tracxn.client import TracxnClient
from pipeline.tracxn.normaliser import (
    normalise_company,
    normalise_founder,
)
from pipeline.models.schema import (
    Company,
    Founder,
    NetworkSnapshot,
)

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"


def _ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, data: Any) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    log.info("Wrote %s", path)


class DataFetcher:
    """Pull and normalise data from Tracxn for a given sector config."""

    def __init__(self, client: TracxnClient | None = None):
        self.client = client or TracxnClient()

    def fetch_sector(self, config: dict) -> NetworkSnapshot:
        """
        Run a full fetch for a sector config.

        Config shape:
            {
                "sector": "FinTech",
                "country": "India",
                "min_funding": 10000000,
                "limit": 200,
                "additional_filters": { ... }
            }
        """
        _ensure_dirs()
        sector = config.get("sector", "Unknown")
        log.info("Fetching sector: %s", sector)

        # 1. Search companies
        raw_companies = self.client.search_companies(
            sector=sector,
            country=config.get("country"),
            city=config.get("city"),
            min_funding=config.get("min_funding"),
            max_funding=config.get("max_funding"),
            founded_after=config.get("founded_after"),
            founded_before=config.get("founded_before"),
            limit=config.get("limit", 100),
        )
        log.info("Fetched %d raw companies", len(raw_companies))

        # Save raw data
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        _write_json(
            RAW_DIR / f"{sector.lower()}_{timestamp}_companies.json",
            raw_companies,
        )

        # 2. Normalise companies
        companies: list[Company] = []
        all_founders: list[Founder] = []
        seen_founders: set[str] = set()

        for raw in raw_companies:
            company = normalise_company(raw)
            companies.append(company)

            # Extract founders from the company's people data
            for person in raw.get("people", raw.get("founders", [])):
                if isinstance(person, dict):
                    founder = normalise_founder(person, company.id)
                    if founder.id not in seen_founders:
                        seen_founders.add(founder.id)
                        all_founders.append(founder)

        # 3. Optionally enrich via individual company lookups
        if config.get("enrich_domains"):
            for company in companies:
                if company.domain:
                    try:
                        detail = self.client.company_lookup(company.domain)
                        # Merge any additional founder data
                        for person in detail.get("people", detail.get("founders", [])):
                            if isinstance(person, dict):
                                f = normalise_founder(person, company.id)
                                if f.id not in seen_founders:
                                    seen_founders.add(f.id)
                                    all_founders.append(f)
                    except Exception:
                        log.warning("Lookup failed for %s", company.domain)

        log.info("Normalised %d companies, %d founders", len(companies), len(all_founders))

        # 4. Build snapshot
        snapshot = NetworkSnapshot(
            sector=sector,
            companies=companies,
            founders=all_founders,
            metadata={
                "config": config,
                "fetched_at": timestamp,
                "raw_count": len(raw_companies),
            },
        )

        # 5. Write processed data
        _write_json(
            PROCESSED_DIR / f"{sector.lower()}_{timestamp}_snapshot.json",
            snapshot.model_dump(),
        )

        return snapshot

    def fetch_transactions(self, company_name: str) -> list[dict]:
        """Fetch funding transactions for a specific company."""
        return self.client.search_transactions(company_name=company_name)

    def fetch_investors(self, name: str) -> list[dict]:
        """Fetch investor data by name."""
        return self.client.search_investors(name=name)
