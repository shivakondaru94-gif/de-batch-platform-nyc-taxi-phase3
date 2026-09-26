"""Generates the Phase 1 architecture diagram as a 300 dpi PNG.

The diagram is code, not a hand-drawn image: it is version-controlled and
regenerates identically with `make diagram`, which is the same
reproducibility argument the platform itself makes.
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")

import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
import matplotlib.pyplot as plt

TITLE = "A Reproducible Batch Data Architecture for Quarterly ML Feature Generation"
SUBTITLE = "Urban Mobility Data (NYC TLC) | all components as isolated Docker microservices | IaC via docker compose"

INK = "#1a1d23"
MUTED = "#5b6472"
EDGE = "#8b95a5"

BANDS = [
    ("SOURCE", "#eef2f7", 0.5, 2.3),
    ("INGESTION", "#e3f0fb", 3.0, 2.6),
    ("STORAGE  (data lake, medallion)", "#e6f4ec", 6.0, 5.4),
    ("PROCESSING", "#fdf0e3", 11.8, 3.0),
    ("SERVING", "#f2e9f7", 15.1, 2.9),
]

# (x, y, w, h, title, subtitle, fill)
BOXES = [
    (0.6, 5.6, 2.1, 1.5, "NYC TLC\nOpen Data", "Parquet, monthly\n~3M rows/month", "#ffffff"),
    (0.6, 3.6, 2.1, 1.3, "Volume", ">36M rows/yr\nevery row timestamped", "#ffffff"),

    (3.1, 5.3, 2.4, 1.8, "ingest-tlc", "Python microservice\nstreamed download\nSHA-256 + idempotent", "#ffffff"),

    (6.1, 7.4, 5.2, 1.15, "BRONZE  -  raw, immutable", "byte-identical source objects, Hive-partitioned, 90d retention", "#ffffff"),
    (6.1, 5.95, 5.2, 1.15, "SILVER  -  validated", "typed, deduplicated, 8 business rules enforced", "#ffffff"),
    (6.1, 4.5, 5.2, 1.15, "GOLD  -  aggregated", "hourly demand features per pickup zone", "#ffffff"),
    (6.1, 3.15, 5.2, 1.05, "QUARANTINE", "rejected rows + reason: data loss stays auditable", "#fdecec"),

    (11.9, 5.6, 2.8, 1.9, "Apache Spark", "1 master + N workers\nscale-out by replica count\nS3A -> MinIO", "#ffffff"),
    (11.9, 3.5, 2.8, 1.7, "Spark jobs", "bronze_to_silver\nsilver_to_gold\ntumbling hourly windows", "#ffffff"),

    (15.2, 5.6, 2.7, 1.9, "PostgreSQL\nfeature store", "ml.features_demand_hourly\nPK (zone, window)\nread-only ML role", "#ffffff"),
    (15.2, 3.5, 2.7, 1.7, "Governance", "batch_lineage\nquality_checks\nfull traceability", "#ffffff"),
]

# Orthogonal routes: each is a list of waypoints, drawn as right-angle
# segments with a single arrowhead on the final leg. Keeps the diagram
# readable - no diagonal lines cutting through component boxes.
ROUTES = [
    # source -> ingestion
    ([(2.75, 6.35), (3.05, 6.35)], None),
    # ingestion -> bronze
    ([(5.55, 6.2), (5.82, 6.2), (5.82, 7.97), (6.05, 7.97)], ("land raw", 5.68, 6.32)),
    # bronze -> spark (read)
    ([(11.35, 7.97), (13.3, 7.97), (13.3, 7.55)], ("read", 12.3, 8.06)),
    # spark master -> jobs
    ([(13.3, 5.55), (13.3, 5.25)], None),
    # jobs -> silver (write validated)
    ([(11.85, 5.05), (11.56, 5.05), (11.56, 6.52), (11.35, 6.52)], None),
    # jobs -> gold (write aggregated)
    ([(11.85, 4.6), (11.72, 4.6), (11.72, 5.07), (11.35, 5.07)], None),
    # jobs -> quarantine (rejected rows)
    ([(11.85, 3.68), (11.35, 3.68)], None),
    # jobs -> serving warehouse (JDBC)
    ([(14.75, 4.9), (14.97, 4.9), (14.97, 6.55), (15.15, 6.55)], ("JDBC", 14.97, 5.6)),
    # jobs -> governance tables
    ([(14.75, 4.0), (15.15, 4.0)], None),
]


def rounded(ax, x, y, w, h, fill, edge=EDGE, lw=1.1, z=3):
    ax.add_patch(
        FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.02,rounding_size=0.14",
            linewidth=lw, edgecolor=edge, facecolor=fill, zorder=z,
        )
    )


def main() -> None:
    fig, ax = plt.subplots(figsize=(18.5, 10))
    ax.set_xlim(0, 18.4)
    ax.set_ylim(0, 10)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    # Layer bands
    for label, colour, x, w in BANDS:
        rounded(ax, x, 2.6, w, 6.3, colour, edge="none", z=1)
        ax.text(
            x + w / 2, 8.68, label, ha="center", va="center",
            fontsize=10.5, fontweight="bold", color=MUTED, zorder=2,
        )

    # Component boxes
    for x, y, w, h, title, sub, fill in BOXES:
        rounded(ax, x, y, w, h, fill)
        ax.text(
            x + w / 2, y + h - 0.32, title, ha="center", va="top",
            fontsize=10.5, fontweight="bold", color=INK, zorder=4,
        )
        ax.text(
            x + w / 2, y + h - 0.78, sub, ha="center", va="top",
            fontsize=8.2, color=MUTED, linespacing=1.5, zorder=4,
        )

    for points, label in ROUTES:
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        # All legs except the last are plain lines...
        if len(points) > 2:
            ax.plot(
                xs[:-1], ys[:-1], color="#4a5568", linewidth=1.4,
                solid_capstyle="round", zorder=5,
            )
        # ...the final leg carries the arrowhead.
        ax.add_patch(
            FancyArrowPatch(
                points[-2], points[-1],
                arrowstyle="-|>", mutation_scale=14,
                linewidth=1.4, color="#4a5568", shrinkA=0, shrinkB=0,
                zorder=5,
            )
        )
        if label:
            text, lx, ly = label
            ax.text(
                lx, ly, text, ha="center", va="bottom", fontsize=7.4,
                color=MUTED, style="italic", zorder=6,
                bbox=dict(facecolor="white", edgecolor="none", pad=1.2),
            )

    # Orchestration spans the whole pipeline
    rounded(ax, 3.0, 1.25, 14.9, 1.05, "#e8eaf6", edge="#9fa8da", z=2)
    ax.text(
        10.45, 1.93, "ORCHESTRATION  -  Apache Airflow",
        ha="center", va="center", fontsize=10.5, fontweight="bold",
        color="#3949ab", zorder=4,
    )
    ax.text(
        10.45, 1.55,
        "monthly_ingest DAG (5th of month)   |   quarterly_features DAG (Jan/Apr/Jul/Oct)   |   "
        "exponential-backoff retries, catchup backfills, lineage writes",
        ha="center", va="center", fontsize=8.4, color=MUTED, zorder=4,
    )

    # Cross-cutting concerns
    rounded(ax, 0.5, 0.15, 17.4, 0.85, "#f5f6f8", edge="#c9cfd8", z=2)
    ax.text(
        0.85, 0.58,
        "CROSS-CUTTING   "
        "Reliability: retries, idempotent ingest, quarantine, healthchecks, object versioning     "
        "Scalability: stateless workers, --scale spark-worker=N, partition pruning     "
        "Maintainability: one compose file, pinned images, Makefile, non-root containers     "
        "Security & governance: .env secrets, scoped lake account, read-only ML role, lineage + quality tables",
        ha="left", va="center", fontsize=7.6, color=MUTED, zorder=4,
    )

    ax.text(0.5, 9.62, TITLE, fontsize=15.5, fontweight="bold", color=INK)
    ax.text(0.5, 9.24, SUBTITLE, fontsize=9.5, color=MUTED)

    legend = [
        mpatches.Patch(facecolor="#e6f4ec", edgecolor="none", label="data lake layer"),
        mpatches.Patch(facecolor="#fdf0e3", edgecolor="none", label="compute"),
        mpatches.Patch(facecolor="#f2e9f7", edgecolor="none", label="serving"),
        mpatches.Patch(facecolor="#e8eaf6", edgecolor="none", label="orchestration"),
    ]
    ax.legend(
        handles=legend, loc="upper right", bbox_to_anchor=(1.0, 0.975),
        ncol=4, frameon=False, fontsize=8.6,
    )

    out = "docs/architecture.png"
    fig.savefig(out, dpi=300, bbox_inches="tight", facecolor="white")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
