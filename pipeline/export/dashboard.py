"""
Dashboard generator.

Takes analysis results and renders an updated HTML dashboard using Jinja2.
Outputs a standalone HTML file (same style as the original) with data
populated from the pipeline rather than hardcoded.

Supports multi-sector dashboards — embed data for multiple sectors into a
single HTML file with a client-side sector selector.
"""

from __future__ import annotations

import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from pipeline.analysis.network import NetworkAnalyser, NetworkInsight
from pipeline.models.schema import Company, Founder

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "templates"
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output"

# ------------------------------------------------------------------
# Sector registry — canonical list of sectors shown in the UI.
# id must match the key used in SECTOR_DATA on the JS side.
# ------------------------------------------------------------------
SECTOR_REGISTRY = [
    {"id": "fintech",       "name": "FinTech",       "icon": "payments",           "color": "#C29D47"},
    {"id": "consumer_tech", "name": "Consumer Tech",  "icon": "devices",            "color": "#2B4C7E"},
    {"id": "edtech",        "name": "EdTech",         "icon": "school",             "color": "#6B3FA0"},
    {"id": "saas",          "name": "SaaS",           "icon": "cloud",              "color": "#0E6245"},
    {"id": "deeptech",      "name": "DeepTech",       "icon": "memory",             "color": "#9F1239"},
    {"id": "climate",       "name": "Climate Tech",   "icon": "eco",                "color": "#0D9488"},
    {"id": "logistics",     "name": "Logistics",      "icon": "local_shipping",     "color": "#D97706"},
    {"id": "healthtech",    "name": "HealthTech",     "icon": "health_and_safety",  "color": "#DC2626"},
]


def _ensure_dirs() -> None:
    TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _sector_id_from_name(sector_name: str) -> str:
    """Derive a sector_id from a display name like 'Indian FinTech'."""
    # Strip country prefix if present
    lower = sector_name.lower()
    for prefix in ("indian ", "india "):
        if lower.startswith(prefix):
            lower = lower[len(prefix):]
            break
    # Match against registry
    for s in SECTOR_REGISTRY:
        if s["name"].lower() == lower or s["id"] == lower.replace(" ", "_"):
            return s["id"]
    # Fallback: slugify
    return lower.replace(" ", "_")


def _build_sector_data(
    sector_name: str,
    companies: list[Company],
    founders: list[Founder],
    analyser: NetworkAnalyser,
) -> dict:
    """Run all analyses and return a dict suitable for embedding as JSON."""
    edu_hubs = analyser.education_hubs(15)
    employer_pipes = analyser.employer_pipelines(15)
    centrality = analyser.centrality_rankings(20)
    clusters = analyser.detect_clusters(3)
    flows = analyser.founder_to_company_flow()[:15]
    geo = analyser.geographic_distribution()
    insights_raw = analyser.generate_insights()
    insights = [
        {"category": i.category, "title": i.title, "detail": i.detail, "score": i.score}
        for i in insights_raw[:20]
    ]

    # Founder matrix
    matrix_data = []
    for f in founders:
        edu_str = ", ".join(
            f"{e.institution}{(' ' + e.degree) if e.degree else ''}{(' ' + str(e.year)) if e.year else ''}"
            for e in f.education
        ) or "\u2014"
        work_str = ", ".join(w.company for w in f.work_history) or "\u2014"
        company_names = []
        for cid in f.companies:
            match = next((c for c in companies if c.id == cid), None)
            company_names.append(match.name if match else cid)

        matrix_data.append({
            "name": f.name,
            "company": ", ".join(company_names),
            "edu": edu_str,
            "work": work_str,
            "tags": f.tags,
            "verified": f.verified,
        })

    total_edges = analyser.graph.number_of_edges() if analyser.graph else 0

    return {
        "sector_name": sector_name,
        "total_companies": len(companies),
        "total_founders": len(founders),
        "total_edges": total_edges,
        "num_clusters": len(clusters),
        "edu_hubs": edu_hubs,
        "employer_pipes": employer_pipes,
        "centrality": centrality,
        "clusters": clusters,
        "flows": flows,
        "geo": geo,
        "matrix": matrix_data,
        "insights": insights,
    }


def generate_dashboard(
    sector_name: str,
    companies: list[Company],
    founders: list[Founder],
    analyser: NetworkAnalyser,
    output_filename: str | None = None,
    *,
    extra_sectors: dict[str, dict] | None = None,
) -> Path:
    """
    Generate an HTML dashboard from analysis results.

    Args:
        sector_name: Display name for the primary sector.
        companies: Company models for the primary sector.
        founders: Founder models for the primary sector.
        analyser: A built NetworkAnalyser for the primary sector.
        output_filename: Optional output file name.
        extra_sectors: Optional mapping of sector_id -> sector_data dict
            (as returned by _build_sector_data) for additional sectors to embed.

    Returns the path to the generated file.
    """
    _ensure_dirs()

    # Build primary sector data
    primary_id = _sector_id_from_name(sector_name)
    primary_data = _build_sector_data(sector_name, companies, founders, analyser)

    # Assemble all sector data
    all_sector_data: dict[str, dict] = {primary_id: primary_data}
    if extra_sectors:
        all_sector_data.update(extra_sectors)

    # Build sectors list for the template (marks which have data)
    sectors_for_template = []
    for s in SECTOR_REGISTRY:
        sectors_for_template.append({
            **s,
            "has_data": s["id"] in all_sector_data,
        })

    # Template context
    context = {
        "page_title": "Founder Networks",
        "active_sector": primary_id,
        "sectors_json": json.dumps(sectors_for_template, default=str),
        "all_sector_data_json": json.dumps(all_sector_data, default=str),
        # Backward-compat flat vars for the primary sector
        "sector_name": sector_name,
        "total_companies": primary_data["total_companies"],
        "total_founders": primary_data["total_founders"],
        "total_edges": primary_data["total_edges"],
        "clusters": primary_data["clusters"],
    }

    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=False)
    template = env.get_template("dashboard.html")
    html = template.render(**context)

    fname = output_filename or f"{sector_name.lower().replace(' ', '_')}_dashboard.html"
    out_path = OUTPUT_DIR / fname
    out_path.write_text(html)
    return out_path


def generate_multi_sector_dashboard(
    sector_datasets: list[tuple[str, list[Company], list[Founder], NetworkAnalyser]],
    output_filename: str = "multi_sector_dashboard.html",
) -> Path:
    """
    Generate a single dashboard with data for multiple sectors.

    Args:
        sector_datasets: List of (sector_name, companies, founders, analyser) tuples.
        output_filename: Output file name.
    """
    if not sector_datasets:
        raise ValueError("At least one sector dataset is required")

    # Build data for every sector
    all_sector_data: dict[str, dict] = {}
    first_id = None
    for sector_name, companies, founders, analyser in sector_datasets:
        sid = _sector_id_from_name(sector_name)
        all_sector_data[sid] = _build_sector_data(sector_name, companies, founders, analyser)
        if first_id is None:
            first_id = sid

    # Use the first sector as the default active one
    primary = all_sector_data[first_id]

    # Generate via the standard path with extra_sectors
    _ensure_dirs()

    sectors_for_template = []
    for s in SECTOR_REGISTRY:
        sectors_for_template.append({
            **s,
            "has_data": s["id"] in all_sector_data,
        })

    context = {
        "page_title": "Founder Networks",
        "active_sector": first_id,
        "sectors_json": json.dumps(sectors_for_template, default=str),
        "all_sector_data_json": json.dumps(all_sector_data, default=str),
        "sector_name": primary["sector_name"],
        "total_companies": primary["total_companies"],
        "total_founders": primary["total_founders"],
        "total_edges": primary["total_edges"],
        "clusters": primary["clusters"],
    }

    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=False)
    template = env.get_template("dashboard.html")
    html = template.render(**context)

    out_path = OUTPUT_DIR / output_filename
    out_path.write_text(html)
    return out_path


def export_json_report(
    sector_name: str,
    analyser: NetworkAnalyser,
    output_filename: str | None = None,
) -> Path:
    """Export the full analysis as a JSON report."""
    _ensure_dirs()

    report = {
        "sector": sector_name,
        "education_hubs": analyser.education_hubs(15),
        "employer_pipelines": analyser.employer_pipelines(15),
        "centrality_rankings": analyser.centrality_rankings(20),
        "clusters": analyser.detect_clusters(3),
        "talent_flows": analyser.founder_to_company_flow()[:15],
        "geographic_distribution": analyser.geographic_distribution(),
        "insights": [
            {"category": i.category, "title": i.title, "detail": i.detail, "score": i.score}
            for i in analyser.generate_insights()
        ],
    }

    fname = output_filename or f"{sector_name.lower().replace(' ', '_')}_report.json"
    out_path = OUTPUT_DIR / fname
    out_path.write_text(json.dumps(report, indent=2, default=str))
    return out_path
