#!/usr/bin/env python3
"""
Founder Network Analysis Pipeline — CLI entry point.

Usage:
  # Analyse existing verified data (no API key needed)
  python run.py analyse --legacy

  # Fetch fresh data from Tracxn and analyse
  python run.py fetch --config configs/fintech_india.yaml

  # Fetch + analyse + generate dashboard
  python run.py full --config configs/fintech_india.yaml

  # Just generate dashboard from last processed snapshot
  python run.py dashboard --sector fintech

  # Generate multi-sector dashboard (legacy + all available snapshots)
  python run.py dashboard --legacy --all-sectors

  # Export JSON report
  python run.py report --legacy
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import click
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("pipeline")


def _load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _load_latest_snapshot(sector: str) -> dict | None:
    """Load the most recent processed snapshot for a sector."""
    processed = Path("data/processed")
    if not processed.exists():
        return None
    files = sorted(processed.glob(f"{sector.lower()}*_snapshot.json"), reverse=True)
    if not files:
        return None
    with open(files[0]) as f:
        return json.load(f)


def _discover_all_snapshots() -> dict[str, dict]:
    """Discover all available processed snapshots, keyed by sector slug."""
    processed = Path("data/processed")
    if not processed.exists():
        return {}
    snapshots: dict[str, dict] = {}
    for f in sorted(processed.glob("*_snapshot.json"), reverse=True):
        # Extract sector slug from filename (e.g., "fintech_20260224_snapshot.json")
        parts = f.stem.split("_")
        sector_slug = parts[0] if parts else f.stem
        if sector_slug not in snapshots:
            with open(f) as fh:
                snapshots[sector_slug] = json.load(fh)
    return snapshots


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

@click.group()
def cli():
    """Founder Network Analysis Pipeline."""
    pass


@cli.command()
@click.option("--legacy", is_flag=True, help="Analyse the original hardcoded verified dataset")
@click.option("--config", "config_path", type=str, help="Path to sector config YAML")
def analyse(legacy: bool, config_path: str | None):
    """Run network analysis and print results to stdout."""
    from pipeline.analysis.network import NetworkAnalyser

    if legacy:
        from pipeline.models.legacy import load_legacy_data
        companies, founders = load_legacy_data()
        log.info("Loaded legacy data: %d companies, %d founders", len(companies), len(founders))
    elif config_path:
        snapshot_data = _load_latest_snapshot(
            _load_config(config_path).get("sector", "unknown")
        )
        if not snapshot_data:
            log.error("No processed snapshot found. Run 'fetch' first.")
            sys.exit(1)
        from pipeline.models.schema import Company, Founder
        companies = [Company(**c) for c in snapshot_data["companies"]]
        founders = [Founder(**f) for f in snapshot_data["founders"]]
    else:
        log.error("Specify --legacy or --config")
        sys.exit(1)

    analyser = NetworkAnalyser(companies, founders)
    analyser.build_graph()

    click.echo("\n=== EDUCATION HUBS ===")
    for h in analyser.education_hubs(15):
        click.echo(f"  {h['institution']:30s}  {h['founder_count']} founders")

    click.echo("\n=== EMPLOYER PIPELINES ===")
    for p in analyser.employer_pipelines(15):
        click.echo(f"  {p['employer']:30s}  {p['founder_count']} founders")

    click.echo("\n=== NETWORK CONNECTORS (betweenness centrality) ===")
    for c in analyser.centrality_rankings(10):
        click.echo(f"  {c['name']:30s}  centrality={c['centrality']}")

    click.echo("\n=== FOUNDER CLUSTERS ===")
    for cl in analyser.detect_clusters(3):
        click.echo(f"  Cluster ({cl['size']} members): {', '.join(cl['members'][:5])}...")

    click.echo("\n=== TOP TALENT FLOWS ===")
    for fl in analyser.founder_to_company_flow()[:10]:
        click.echo(f"  {fl['from_employer']:20s} -> {fl['to_company']:20s}  ({fl['count']} founders)")

    click.echo("\n=== TOP INSIGHTS ===")
    for ins in analyser.generate_insights()[:10]:
        click.echo(f"  [{ins.category}] {ins.title}")
        click.echo(f"    {ins.detail}\n")


@cli.command()
@click.option("--config", "config_path", required=True, type=str, help="Path to sector config YAML")
def fetch(config_path: str):
    """Fetch data from Tracxn API and save processed snapshots."""
    config = _load_config(config_path)
    from pipeline.tracxn.fetcher import DataFetcher

    fetcher = DataFetcher()
    snapshot = fetcher.fetch_sector(config.get("filters", config))
    log.info(
        "Fetch complete: %d companies, %d founders",
        len(snapshot.companies),
        len(snapshot.founders),
    )


@cli.command()
@click.option("--legacy", is_flag=True, help="Use the original verified dataset")
@click.option("--config", "config_path", type=str, help="Path to sector config YAML")
@click.option("--all-sectors", is_flag=True, help="Bundle all available sector data into one dashboard")
@click.option("--output", "output_name", type=str, help="Output filename")
def dashboard(legacy: bool, config_path: str | None, all_sectors: bool, output_name: str | None):
    """Generate an HTML dashboard."""
    from pipeline.analysis.network import NetworkAnalyser
    from pipeline.export.dashboard import (
        generate_dashboard,
        generate_multi_sector_dashboard,
        _build_sector_data,
        _sector_id_from_name,
    )

    if all_sectors:
        # Multi-sector mode: gather all available data
        sector_datasets = []

        # Always include legacy as FinTech if --legacy is set
        if legacy:
            from pipeline.models.legacy import load_legacy_data
            companies, founders = load_legacy_data()
            analyser = NetworkAnalyser(companies, founders)
            analyser.build_graph()
            sector_datasets.append(("Indian FinTech", companies, founders, analyser))
            log.info("Loaded legacy FinTech: %d companies, %d founders", len(companies), len(founders))

        # Discover processed snapshots
        from pipeline.models.schema import Company, Founder
        for slug, snapshot_data in _discover_all_snapshots().items():
            # Skip if we already loaded this sector from legacy
            if legacy and slug == "fintech":
                continue
            try:
                companies = [Company(**c) for c in snapshot_data.get("companies", [])]
                founders = [Founder(**f) for f in snapshot_data.get("founders", [])]
                sector_name = snapshot_data.get("sector", slug.replace("_", " ").title())
                analyser = NetworkAnalyser(companies, founders)
                analyser.build_graph()
                sector_datasets.append((sector_name, companies, founders, analyser))
                log.info("Loaded snapshot %s: %d companies, %d founders", slug, len(companies), len(founders))
            except Exception as e:
                log.warning("Skipping snapshot %s: %s", slug, e)

        if not sector_datasets:
            log.error("No sector data available. Use --legacy or run 'fetch' first.")
            sys.exit(1)

        out = generate_multi_sector_dashboard(
            sector_datasets,
            output_filename=output_name or "multi_sector_dashboard.html",
        )
        click.echo(f"Multi-sector dashboard generated: {out}")

    else:
        # Single-sector mode (original behavior)
        if legacy:
            from pipeline.models.legacy import load_legacy_data
            companies, founders = load_legacy_data()
            sector_name = "Indian FinTech"
        elif config_path:
            config = _load_config(config_path)
            sector_name = config.get("name", config.get("sector", "Unknown"))
            snapshot_data = _load_latest_snapshot(config.get("sector", "unknown"))
            if not snapshot_data:
                log.error("No processed snapshot found. Run 'fetch' first.")
                sys.exit(1)
            from pipeline.models.schema import Company, Founder
            companies = [Company(**c) for c in snapshot_data["companies"]]
            founders = [Founder(**f) for f in snapshot_data["founders"]]
        else:
            log.error("Specify --legacy or --config")
            sys.exit(1)

        analyser = NetworkAnalyser(companies, founders)
        analyser.build_graph()

        out = generate_dashboard(sector_name, companies, founders, analyser, output_name)
        click.echo(f"Dashboard generated: {out}")


@cli.command()
@click.option("--legacy", is_flag=True, help="Use the original verified dataset")
@click.option("--config", "config_path", type=str, help="Path to sector config YAML")
@click.option("--output", "output_name", type=str, help="Output filename")
def report(legacy: bool, config_path: str | None, output_name: str | None):
    """Export a JSON analysis report."""
    from pipeline.analysis.network import NetworkAnalyser
    from pipeline.export.dashboard import export_json_report

    if legacy:
        from pipeline.models.legacy import load_legacy_data
        companies, founders = load_legacy_data()
        sector_name = "Indian FinTech"
    elif config_path:
        config = _load_config(config_path)
        sector_name = config.get("name", config.get("sector", "Unknown"))
        snapshot_data = _load_latest_snapshot(config.get("sector", "unknown"))
        if not snapshot_data:
            log.error("No processed snapshot found. Run 'fetch' first.")
            sys.exit(1)
        from pipeline.models.schema import Company, Founder
        companies = [Company(**c) for c in snapshot_data["companies"]]
        founders = [Founder(**f) for f in snapshot_data["founders"]]
    else:
        log.error("Specify --legacy or --config")
        sys.exit(1)

    analyser = NetworkAnalyser(companies, founders)
    analyser.build_graph()

    out = export_json_report(sector_name, analyser, output_name)
    click.echo(f"Report exported: {out}")


@cli.command()
@click.option("--config", "config_path", required=True, type=str, help="Path to sector config YAML")
@click.option("--output", "output_name", type=str, help="Output filename")
def full(config_path: str, output_name: str | None):
    """Full pipeline: fetch from Tracxn → analyse → generate dashboard."""
    config = _load_config(config_path)
    sector_name = config.get("name", config.get("sector", "Unknown"))

    # Fetch
    from pipeline.tracxn.fetcher import DataFetcher
    fetcher = DataFetcher()
    snapshot = fetcher.fetch_sector(config.get("filters", config))
    log.info("Fetched %d companies, %d founders", len(snapshot.companies), len(snapshot.founders))

    # Analyse
    from pipeline.analysis.network import NetworkAnalyser
    analyser = NetworkAnalyser(snapshot.companies, snapshot.founders)
    analyser.build_graph()

    # Dashboard
    from pipeline.export.dashboard import generate_dashboard, export_json_report
    dash = generate_dashboard(sector_name, snapshot.companies, snapshot.founders, analyser, output_name)
    rpt = export_json_report(sector_name, analyser)
    click.echo(f"Dashboard: {dash}")
    click.echo(f"Report: {rpt}")


if __name__ == "__main__":
    cli()
