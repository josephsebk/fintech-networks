"""
Tracxn Playground API client.

Wraps the Tracxn REST API v2.2 Playground for company, founder, investor,
and transaction data retrieval.  Handles auth, pagination, rate-limit
back-off, and response normalisation.

Playground API specifics (derived from the MCP server reference impl):
  - Base URL:    https://platform.tracxn.com/api/2.2/playground
  - Auth header: accesstoken (lowercase)
  - Pagination:  {"size": 20, "from": 0}  (max 20 per page)
  - Filters:     {"filter": {"feedName": ["FinTech"], ...}}
  - Endpoints:   companies, transactions, investors,
                 acquisitiontransactions, practiceareas, feeds, businessmodels

Requires TRACXN_ACCESS_TOKEN env var (or pass token directly).
"""

from __future__ import annotations

import os
import time
import logging
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

PLAYGROUND_BASE = "https://platform.tracxn.com/api/2.2/playground"
MAX_PER_PAGE = 20  # Tracxn hard limit


class TracxnAPIError(Exception):
    """Raised on non-recoverable API errors."""

    def __init__(self, status: int, detail: str):
        self.status = status
        self.detail = detail
        super().__init__(f"Tracxn API {status}: {detail}")


class TracxnClient:
    """Thin wrapper around the Tracxn Playground REST API v2.2."""

    def __init__(
        self,
        token: str | None = None,
        base_url: str | None = None,
        max_retries: int = 4,
    ):
        self.token = token or os.getenv("TRACXN_ACCESS_TOKEN", "")
        if not self.token:
            raise ValueError(
                "Tracxn access token required. Set TRACXN_ACCESS_TOKEN "
                "in your environment or pass token= to TracxnClient()."
            )
        self.base = (
            base_url or os.getenv("TRACXN_API_BASE", PLAYGROUND_BASE)
        ).rstrip("/")
        self.max_retries = max_retries
        self._session = requests.Session()
        self._session.headers.update(
            {
                "accesstoken": self.token,  # lowercase per Playground spec
                "Content-Type": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Low-level request with retry + exponential back-off
    # ------------------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs: Any) -> dict:
        url = f"{self.base}/{path.lstrip('/')}"
        attempt = 0
        while True:
            try:
                log.debug("→ %s %s", method, url)
                resp = self._session.request(method, url, **kwargs)
            except requests.RequestException as exc:
                attempt += 1
                if attempt > self.max_retries:
                    raise
                wait = 2**attempt
                log.warning("Network error (%s), retrying in %ss…", exc, wait)
                time.sleep(wait)
                continue

            if resp.status_code == 429:
                attempt += 1
                if attempt > self.max_retries:
                    raise TracxnAPIError(429, "Rate limited — max retries exceeded")
                wait = 2**attempt
                log.warning("Rate limited, retrying in %ss…", wait)
                time.sleep(wait)
                continue

            if resp.status_code >= 400:
                raise TracxnAPIError(resp.status_code, resp.text[:500])

            return resp.json()

    def _post(self, path: str, payload: dict) -> dict:
        return self._request("POST", path, json=payload)

    def _get(self, path: str, params: dict | None = None) -> dict:
        return self._request("GET", path, params=params)

    # ------------------------------------------------------------------
    # Pagination helper (Playground uses "size" / "from")
    # ------------------------------------------------------------------

    def _paginate(self, path: str, payload: dict, limit: int = 100) -> list[dict]:
        """Fetch up to *limit* results, handling Tracxn's 20-per-page cap."""
        results: list[dict] = []
        offset = payload.get("from", 0)
        while len(results) < limit:
            batch_size = min(MAX_PER_PAGE, limit - len(results))
            payload["size"] = batch_size
            payload["from"] = offset
            data = self._post(path, payload)

            # Response may nest items under "result", "items", or at top level
            items = data.get("result", data.get("items", []))
            if isinstance(items, dict):
                # Some endpoints return {"result": {"items": [...]}}
                items = items.get("items", [])
            if not items:
                break

            results.extend(items)
            offset += len(items)
            if len(items) < batch_size:
                break  # last page

        return results[:limit]

    # ------------------------------------------------------------------
    # Company endpoints
    # ------------------------------------------------------------------

    def search_companies(
        self,
        *,
        sector: str | None = None,
        city: str | None = None,
        country: str | None = None,
        min_funding: int | None = None,
        max_funding: int | None = None,
        founded_after: int | None = None,
        founded_before: int | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """
        Search companies with filters.

        Playground uses "feedName" (list) for sector filtering.
        """
        filters: dict[str, Any] = {}
        if sector:
            filters["feedName"] = [sector]
        if city:
            filters["city"] = city
        if country:
            filters["country"] = country
        if min_funding is not None or max_funding is not None:
            funding: dict[str, int] = {}
            if min_funding is not None:
                funding["min"] = min_funding
            if max_funding is not None:
                funding["max"] = max_funding
            filters["totalFundingAmount"] = funding
        if founded_after or founded_before:
            year_range: dict[str, int] = {}
            if founded_after:
                year_range["min"] = founded_after
            if founded_before:
                year_range["max"] = founded_before
            filters["yearFounded"] = year_range

        payload: dict[str, Any] = {"filter": filters}
        return self._paginate("companies", payload, limit=limit)

    def search_companies_by_name(self, name: str, limit: int = 20) -> list[dict]:
        """Name-based company search."""
        return self._paginate(
            "companies",
            {"filter": {"name": name}},
            limit=limit,
        )

    def company_lookup(self, domain: str) -> dict:
        """
        Lookup a specific company by its website domain.

        Domain should be bare (e.g. "razorpay.com", not "https://razorpay.com").
        """
        return self._post("companies", {"filter": {"domain": [domain]}})

    def get_funded_companies(
        self,
        min_amount: int = 0,
        max_amount: int | None = None,
        sector: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Retrieve companies within a funding range."""
        filters: dict[str, Any] = {
            "totalFundingAmount": {"min": min_amount},
        }
        if max_amount:
            filters["totalFundingAmount"]["max"] = max_amount
        if sector:
            filters["feedName"] = [sector]
        return self._paginate("companies", {"filter": filters}, limit=limit)

    # ------------------------------------------------------------------
    # Transaction / funding endpoints
    # ------------------------------------------------------------------

    def search_transactions(
        self,
        *,
        company_name: str | None = None,
        round_type: str | None = None,
        min_amount: int | None = None,
        max_amount: int | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Search funding transactions."""
        filters: dict[str, Any] = {}
        if company_name:
            filters["companyName"] = company_name
        if round_type:
            filters["roundType"] = round_type
        if min_amount is not None or max_amount is not None:
            amt: dict[str, int] = {}
            if min_amount is not None:
                amt["min"] = min_amount
            if max_amount is not None:
                amt["max"] = max_amount
            filters["amount"] = amt
        return self._paginate("transactions", {"filter": filters}, limit=limit)

    # ------------------------------------------------------------------
    # Investor endpoints
    # ------------------------------------------------------------------

    def search_investors(
        self,
        *,
        name: str | None = None,
        investor_type: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Search investors."""
        filters: dict[str, Any] = {}
        if name:
            filters["name"] = name
        if investor_type:
            filters["type"] = investor_type
        return self._paginate("investors", {"filter": filters}, limit=limit)

    # ------------------------------------------------------------------
    # Acquisitions
    # ------------------------------------------------------------------

    def search_acquisitions(
        self,
        *,
        acquirer: str | None = None,
        target: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Search acquisition deals."""
        filters: dict[str, Any] = {}
        if acquirer:
            filters["acquirerName"] = acquirer
        if target:
            filters["targetName"] = target
        return self._paginate(
            "acquisitiontransactions", {"filter": filters}, limit=limit
        )

    # ------------------------------------------------------------------
    # Taxonomy: practice areas, feeds, business models
    # ------------------------------------------------------------------

    def search_practice_areas(self, query: str, limit: int = 20) -> list[dict]:
        """Search Tracxn's practice area taxonomy."""
        return self._paginate(
            "practiceareas",
            {"filter": {"name": query}},
            limit=limit,
        )

    def search_feeds(self, query: str, limit: int = 20) -> list[dict]:
        """Search Tracxn feed names (their sector taxonomy)."""
        return self._paginate(
            "feeds",
            {"filter": {"name": query}},
            limit=limit,
        )

    def search_business_models(self, query: str, limit: int = 20) -> list[dict]:
        """Search business model classifications."""
        return self._paginate(
            "businessmodels",
            {"filter": {"name": query}},
            limit=limit,
        )
