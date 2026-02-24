"""
Network analysis engine.

Builds a NetworkX graph from companies + founders, detects clusters,
computes centrality metrics, and answers the "deeper questions":
  - Which institutions/employers are the strongest founder factories?
  - What are the tightest co-founder clusters?
  - Which founders bridge multiple networks?
  - What are the emerging "mafias"?
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field

import networkx as nx

from pipeline.models.schema import (
    Company,
    Founder,
    NetworkEdge,
    RelationshipType,
)

log = logging.getLogger(__name__)


@dataclass
class NetworkInsight:
    """A single insight produced by the analysis engine."""
    category: str  # e.g. "education_hub", "employer_pipeline", "cluster"
    title: str
    detail: str
    score: float = 0.0
    entities: list[str] = field(default_factory=list)


class NetworkAnalyser:
    """Build and analyse the founder network graph."""

    def __init__(
        self,
        companies: list[Company],
        founders: list[Founder],
    ):
        self.companies = {c.id: c for c in companies}
        self.founders = {f.id: f for f in founders}
        self.graph = nx.Graph()
        self.edges: list[NetworkEdge] = []

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def build_graph(self) -> nx.Graph:
        """Construct the full network graph from founders + companies."""
        # Add founder nodes
        for fid, f in self.founders.items():
            attrs = f.model_dump()
            attrs.pop("id", None)
            self.graph.add_node(fid, node_type="founder", **attrs)

        # 1. Co-founder edges (same company)
        company_founders: dict[str, list[str]] = defaultdict(list)
        for fid, f in self.founders.items():
            for cid in f.companies:
                company_founders[cid].append(fid)

        for cid, fids in company_founders.items():
            for i, a in enumerate(fids):
                for b in fids[i + 1 :]:
                    edge = NetworkEdge(
                        source_id=a,
                        target_id=b,
                        relationship=RelationshipType.CO_FOUNDER,
                        weight=3.0,
                        context=self.companies.get(cid, Company(id=cid, name=cid)).name,
                    )
                    self.edges.append(edge)
                    self.graph.add_edge(a, b, weight=3.0, rel="co_founder", ctx=cid)

        # 2. Same-college edges
        institution_map: dict[str, list[str]] = defaultdict(list)
        for fid, f in self.founders.items():
            for ed in f.education:
                # Normalise institution name
                inst = ed.institution.strip()
                if inst:
                    institution_map[inst].append(fid)

        for inst, fids in institution_map.items():
            unique = list(set(fids))
            for i, a in enumerate(unique):
                for b in unique[i + 1 :]:
                    # Don't double-count if already co-founders
                    if self.graph.has_edge(a, b):
                        self.graph[a][b]["weight"] += 1.0
                    else:
                        self.graph.add_edge(a, b, weight=1.0, rel="same_college", ctx=inst)
                    self.edges.append(
                        NetworkEdge(
                            source_id=a,
                            target_id=b,
                            relationship=RelationshipType.SAME_COLLEGE,
                            weight=1.0,
                            context=inst,
                        )
                    )

        # 3. Same-employer edges
        employer_map: dict[str, list[str]] = defaultdict(list)
        for fid, f in self.founders.items():
            for w in f.work_history:
                emp = w.company.strip()
                if emp and emp != "—":
                    employer_map[emp].append(fid)

        for emp, fids in employer_map.items():
            unique = list(set(fids))
            for i, a in enumerate(unique):
                for b in unique[i + 1 :]:
                    if self.graph.has_edge(a, b):
                        self.graph[a][b]["weight"] += 1.5
                    else:
                        self.graph.add_edge(a, b, weight=1.5, rel="same_employer", ctx=emp)
                    self.edges.append(
                        NetworkEdge(
                            source_id=a,
                            target_id=b,
                            relationship=RelationshipType.SAME_EMPLOYER,
                            weight=1.5,
                            context=emp,
                        )
                    )

        log.info(
            "Graph built: %d nodes, %d edges",
            self.graph.number_of_nodes(),
            self.graph.number_of_edges(),
        )
        return self.graph

    # ------------------------------------------------------------------
    # Analysis methods
    # ------------------------------------------------------------------

    def education_hubs(self, top_n: int = 15) -> list[dict]:
        """Rank institutions by number of founders produced."""
        counter: Counter[str] = Counter()
        for f in self.founders.values():
            for ed in f.education:
                if ed.institution.strip():
                    counter[ed.institution.strip()] += 1
        return [
            {"institution": inst, "founder_count": count}
            for inst, count in counter.most_common(top_n)
        ]

    def employer_pipelines(self, top_n: int = 15) -> list[dict]:
        """Rank prior employers by number of founders they spawned."""
        counter: Counter[str] = Counter()
        for f in self.founders.values():
            for w in f.work_history:
                emp = w.company.strip()
                if emp and emp != "—":
                    counter[emp] += 1
        return [
            {"employer": emp, "founder_count": count}
            for emp, count in counter.most_common(top_n)
        ]

    def centrality_rankings(self, top_n: int = 20) -> list[dict]:
        """
        Rank founders by betweenness centrality — identifies founders
        who bridge multiple networks (the "connectors").
        """
        if not self.graph.edges:
            return []
        bc = nx.betweenness_centrality(self.graph, weight="weight")
        ranked = sorted(bc.items(), key=lambda x: x[1], reverse=True)[:top_n]
        return [
            {
                "founder_id": fid,
                "name": self.founders[fid].name if fid in self.founders else fid,
                "centrality": round(score, 4),
            }
            for fid, score in ranked
        ]

    def detect_clusters(self, min_size: int = 3) -> list[dict]:
        """
        Detect tightly connected founder clusters using community detection.
        These are potential "mafias" — groups with overlapping education,
        employers, or co-founding history.
        """
        if not self.graph.edges:
            return []
        communities = nx.community.greedy_modularity_communities(self.graph)
        clusters = []
        for i, community in enumerate(communities):
            if len(community) < min_size:
                continue
            members = list(community)
            # Determine the dominant shared context
            contexts: Counter[str] = Counter()
            for fid in members:
                if fid in self.founders:
                    f = self.founders[fid]
                    for ed in f.education:
                        contexts[ed.institution] += 1
                    for w in f.work_history:
                        contexts[w.company] += 1
                    for cid in f.companies:
                        c = self.companies.get(cid)
                        if c:
                            contexts[c.name] += 1

            top_context = contexts.most_common(3)
            clusters.append({
                "cluster_id": i,
                "size": len(members),
                "members": [
                    self.founders[m].name if m in self.founders else m
                    for m in members
                ],
                "dominant_context": [
                    {"entity": ctx, "count": cnt} for ctx, cnt in top_context
                ],
            })
        return sorted(clusters, key=lambda c: c["size"], reverse=True)

    def founder_to_company_flow(self) -> list[dict]:
        """
        Trace employer → founded-company pipelines.
        e.g. "Flipkart → PhonePe (4 founders)"
        """
        flows: dict[tuple[str, str], list[str]] = defaultdict(list)
        for fid, f in self.founders.items():
            employers = {w.company.strip() for w in f.work_history if w.company.strip() != "—"}
            founded = set(f.companies)
            for emp in employers:
                for comp_id in founded:
                    c = self.companies.get(comp_id)
                    comp_name = c.name if c else comp_id
                    flows[(emp, comp_name)].append(f.name)

        results = [
            {
                "from_employer": emp,
                "to_company": comp,
                "founders": names,
                "count": len(names),
            }
            for (emp, comp), names in flows.items()
            if len(names) >= 2
        ]
        return sorted(results, key=lambda r: r["count"], reverse=True)

    def geographic_distribution(self) -> list[dict]:
        """City-level distribution of companies."""
        counter: Counter[str] = Counter()
        for c in self.companies.values():
            city = c.city or "Unknown"
            counter[city] += 1
        total = sum(counter.values())
        return [
            {
                "city": city,
                "count": count,
                "pct": round(100 * count / total, 1) if total else 0,
            }
            for city, count in counter.most_common()
        ]

    def generate_insights(self) -> list[NetworkInsight]:
        """Run all analyses and produce a list of ranked insights."""
        insights: list[NetworkInsight] = []

        # Education hubs
        for hub in self.education_hubs(10):
            if hub["founder_count"] >= 3:
                insights.append(
                    NetworkInsight(
                        category="education_hub",
                        title=f"{hub['institution']} Founder Factory",
                        detail=f"{hub['institution']} produced {hub['founder_count']} founders in this sector.",
                        score=hub["founder_count"],
                        entities=[hub["institution"]],
                    )
                )

        # Employer pipelines
        for pipe in self.employer_pipelines(10):
            if pipe["founder_count"] >= 3:
                insights.append(
                    NetworkInsight(
                        category="employer_pipeline",
                        title=f"{pipe['employer']} Alumni Mafia",
                        detail=f"{pipe['employer']} spawned {pipe['founder_count']} founders.",
                        score=pipe["founder_count"] * 1.5,
                        entities=[pipe["employer"]],
                    )
                )

        # Bridge founders (high centrality)
        for bridge in self.centrality_rankings(5):
            if bridge["centrality"] > 0.01:
                insights.append(
                    NetworkInsight(
                        category="bridge_founder",
                        title=f"{bridge['name']} — Network Connector",
                        detail=(
                            f"{bridge['name']} has betweenness centrality "
                            f"{bridge['centrality']}, indicating they bridge "
                            f"multiple founder networks."
                        ),
                        score=bridge["centrality"] * 100,
                        entities=[bridge["name"]],
                    )
                )

        # Clusters / mafias
        for cluster in self.detect_clusters(3):
            top_ctx = cluster["dominant_context"][0]["entity"] if cluster["dominant_context"] else "unknown"
            insights.append(
                NetworkInsight(
                    category="cluster",
                    title=f"{top_ctx} Cluster ({cluster['size']} founders)",
                    detail=f"Tight cluster of {cluster['size']} founders dominated by {top_ctx} connections.",
                    score=cluster["size"] * 2,
                    entities=cluster["members"],
                )
            )

        # Employer → Company flows
        for flow in self.founder_to_company_flow()[:10]:
            insights.append(
                NetworkInsight(
                    category="talent_flow",
                    title=f"{flow['from_employer']} → {flow['to_company']}",
                    detail=(
                        f"{flow['count']} founders moved from {flow['from_employer']} "
                        f"to found {flow['to_company']}: {', '.join(flow['founders'])}."
                    ),
                    score=flow["count"] * 2.5,
                    entities=flow["founders"],
                )
            )

        return sorted(insights, key=lambda i: i.score, reverse=True)
