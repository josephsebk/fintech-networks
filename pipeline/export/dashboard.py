"""
Dashboard generator.

Takes analysis results and renders an updated HTML dashboard using Jinja2.
Outputs a standalone HTML file (same style as the original) with data
populated from the pipeline rather than hardcoded.
"""

from __future__ import annotations

import json
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from pipeline.analysis.network import NetworkAnalyser, NetworkInsight
from pipeline.models.schema import Company, Founder

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "templates"
OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output"


def _ensure_dirs() -> None:
    TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def generate_dashboard(
    sector_name: str,
    companies: list[Company],
    founders: list[Founder],
    analyser: NetworkAnalyser,
    output_filename: str | None = None,
) -> Path:
    """
    Generate an HTML dashboard from analysis results.

    Returns the path to the generated file.
    """
    _ensure_dirs()

    # Run all analyses
    edu_hubs = analyser.education_hubs(15)
    employer_pipes = analyser.employer_pipelines(15)
    centrality = analyser.centrality_rankings(20)
    clusters = analyser.detect_clusters(3)
    flows = analyser.founder_to_company_flow()[:15]
    geo = analyser.geographic_distribution()
    insights = analyser.generate_insights()

    # Prepare founder matrix data (matching original format)
    matrix_data = []
    for f in founders:
        edu_str = ", ".join(
            f"{e.institution}{(' ' + e.degree) if e.degree else ''}{(' ' + str(e.year)) if e.year else ''}"
            for e in f.education
        ) or "—"
        work_str = ", ".join(w.company for w in f.work_history) or "—"
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

    # Template context
    context = {
        "sector_name": sector_name,
        "total_companies": len(companies),
        "total_founders": len(founders),
        "matrix_data": matrix_data,
        "matrix_json": json.dumps(matrix_data, default=str),
        "edu_hubs": edu_hubs,
        "edu_hubs_json": json.dumps(edu_hubs),
        "employer_pipes": employer_pipes,
        "employer_pipes_json": json.dumps(employer_pipes),
        "centrality": centrality,
        "centrality_json": json.dumps(centrality, default=str),
        "clusters": clusters,
        "clusters_json": json.dumps(clusters, default=str),
        "flows": flows,
        "flows_json": json.dumps(flows, default=str),
        "geo": geo,
        "geo_json": json.dumps(geo),
        "insights": [
            {"category": i.category, "title": i.title, "detail": i.detail, "score": i.score}
            for i in insights[:20]
        ],
        "insights_json": json.dumps(
            [{"category": i.category, "title": i.title, "detail": i.detail, "score": i.score}
             for i in insights[:20]],
            default=str,
        ),
    }

    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)
    template = env.get_template("dashboard.html")
    html = template.render(**context)

    fname = output_filename or f"{sector_name.lower().replace(' ', '_')}_dashboard.html"
    out_path = OUTPUT_DIR / fname
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
