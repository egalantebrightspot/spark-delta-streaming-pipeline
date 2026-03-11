"""Pipeline observability dashboard.

Queries the Bronze, Silver, and Gold Delta tables to produce a
standalone HTML report with Plotly charts covering:

  1. Pipeline throughput (rows per layer)
  2. Anomaly spikes (by device and type)
  3. Delta table growth (version history, file counts, sizes)
  4. Processing latency (ingestion-to-processing delay)
  5. Device health overview (score distribution, risk tiers)
  6. Data quality distribution

All Spark operations use collect()/toPandas() on small aggregated
result sets -- no Python workers required.
"""

from __future__ import annotations

import html
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    round as spark_round,
    unix_timestamp,
)

from pipeline.common.logging_config import get_logger
from pipeline.gold_aggregations.gold_aggregations_job import gold_table_path

logger = get_logger("monitoring.dashboard")

TIER_COLORS = {"healthy": "#2ecc71", "warning": "#f39c12", "critical": "#e74c3c"}
LAYER_COLORS = {"Bronze": "#cd7f32", "Silver": "#c0c0c0", "Gold": "#ffd700"}


# ── Data Collection ──────────────────────────────────────────────────────────


def collect_pipeline_metrics(spark: SparkSession, config: dict) -> dict:
    """Gather all observability data from Delta tables into pandas frames."""
    paths = config["paths"]
    metrics: dict = {}

    # Layer row counts
    layer_counts = {}
    for name, path in [
        ("Bronze", paths["delta_bronze"]),
        ("Silver", paths["delta_silver"]),
    ]:
        try:
            layer_counts[name] = spark.read.format("delta").load(path).count()
        except Exception:
            layer_counts[name] = 0

    gold_tables = ["device_summary", "anomaly_summary", "device_health", "ml_features"]
    gold_total = 0
    for t in gold_tables:
        try:
            gold_total += spark.read.format("delta").load(
                gold_table_path(config, t)
            ).count()
        except Exception:
            pass
    layer_counts["Gold"] = gold_total
    metrics["layer_counts"] = layer_counts

    # Delta version history per layer
    from delta.tables import DeltaTable

    histories = {}
    for name, path in [
        ("Bronze", paths["delta_bronze"]),
        ("Silver", paths["delta_silver"]),
    ]:
        try:
            dt = DeltaTable.forPath(spark, path)
            hist_df = dt.history().select(
                "version", "timestamp", "operation",
                col("operationMetrics.numOutputRows").alias("rows_written"),
                col("operationMetrics.numFiles").alias("num_files"),
                col("operationMetrics.numOutputBytes").alias("bytes_written"),
            )
            histories[name] = hist_df.toPandas()
        except Exception:
            histories[name] = pd.DataFrame()
    for t in gold_tables:
        try:
            dt = DeltaTable.forPath(spark, gold_table_path(config, t))
            hist_df = dt.history().select(
                "version", "timestamp", "operation",
                col("operationMetrics.numOutputRows").alias("rows_written"),
            )
            histories[f"Gold/{t}"] = hist_df.toPandas()
        except Exception:
            pass
    metrics["histories"] = histories

    # Anomaly summary from Gold
    try:
        anm = spark.read.format("delta").load(
            gold_table_path(config, "anomaly_summary")
        )
        metrics["anomaly_summary"] = anm.toPandas()
    except Exception:
        metrics["anomaly_summary"] = pd.DataFrame()

    # Device health from Gold
    try:
        dh = spark.read.format("delta").load(
            gold_table_path(config, "device_health")
        )
        metrics["device_health"] = dh.toPandas()
    except Exception:
        metrics["device_health"] = pd.DataFrame()

    # Device summary from Gold
    try:
        ds = spark.read.format("delta").load(
            gold_table_path(config, "device_summary")
        )
        metrics["device_summary"] = ds.toPandas()
    except Exception:
        metrics["device_summary"] = pd.DataFrame()

    # Processing latency from Silver
    try:
        silver = spark.read.format("delta").load(paths["delta_silver"])
        latency_df = (
            silver
            .withColumn(
                "_latency_sec",
                unix_timestamp("_processed_at") - unix_timestamp("_ingested_at"),
            )
            .select(
                "device_id",
                "_ingested_at", "_processed_at",
                "_latency_sec", "_quality_score", "_is_anomaly",
            )
        )
        metrics["latency"] = latency_df.toPandas()
    except Exception:
        metrics["latency"] = pd.DataFrame()

    return metrics


# ── Chart Builders ───────────────────────────────────────────────────────────


def fig_throughput(metrics: dict) -> go.Figure:
    """Row counts flowing through each pipeline layer."""
    counts = metrics["layer_counts"]
    layers = list(counts.keys())
    values = list(counts.values())
    colors = [LAYER_COLORS.get(l, "#888") for l in layers]

    fig = go.Figure(go.Bar(
        x=layers, y=values,
        marker_color=colors,
        text=values, textposition="outside",
    ))
    fig.update_layout(
        title="Pipeline Throughput: Rows per Layer",
        yaxis_title="Row Count",
        template="plotly_white",
        height=350,
    )
    return fig


def fig_anomaly_spikes(metrics: dict) -> go.Figure:
    """Anomaly counts by device and type."""
    df = metrics.get("anomaly_summary", pd.DataFrame())
    if df.empty:
        return _empty_fig("Anomaly Spikes", "No anomaly data available")

    agg = df.groupby("device_id").agg({
        "temp_anomaly_count": "sum",
        "humidity_anomaly_count": "sum",
        "pressure_anomaly_count": "sum",
        "total_anomaly_count": "sum",
    }).reset_index()
    agg = agg[agg["total_anomaly_count"] > 0].sort_values(
        "total_anomaly_count", ascending=True,
    )

    if agg.empty:
        return _empty_fig("Anomaly Spikes", "No anomalies detected")

    fig = go.Figure()
    for col_name, label, color in [
        ("temp_anomaly_count", "Temperature", "#e74c3c"),
        ("humidity_anomaly_count", "Humidity", "#3498db"),
        ("pressure_anomaly_count", "Pressure", "#9b59b6"),
    ]:
        fig.add_trace(go.Bar(
            y=agg["device_id"], x=agg[col_name],
            name=label, orientation="h",
            marker_color=color,
        ))
    fig.update_layout(
        title="Anomaly Spikes by Device and Type",
        xaxis_title="Anomaly Count",
        barmode="stack",
        template="plotly_white",
        height=max(300, len(agg) * 30 + 100),
    )
    return fig


def fig_delta_growth(metrics: dict) -> go.Figure:
    """Delta table version history timeline."""
    histories = metrics.get("histories", {})
    if not histories:
        return _empty_fig("Delta Table Growth", "No history data")

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=["Versions Over Time", "Rows Written per Commit"],
    )

    for name, hdf in histories.items():
        if hdf.empty:
            continue
        hdf = hdf.sort_values("version")
        hdf["rows_written"] = pd.to_numeric(hdf["rows_written"], errors="coerce")

        fig.add_trace(
            go.Scatter(
                x=hdf["timestamp"], y=hdf["version"],
                mode="lines+markers", name=name,
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Bar(
                x=hdf["timestamp"],
                y=hdf["rows_written"],
                name=f"{name} rows",
                showlegend=False,
            ),
            row=1, col=2,
        )

    fig.update_layout(
        title="Delta Table Growth",
        template="plotly_white",
        height=400,
    )
    fig.update_xaxes(title_text="Time", row=1, col=1)
    fig.update_xaxes(title_text="Time", row=1, col=2)
    fig.update_yaxes(title_text="Version", row=1, col=1)
    fig.update_yaxes(title_text="Rows Written", row=1, col=2)
    return fig


def fig_batch_latency(metrics: dict) -> go.Figure:
    """Distribution of ingestion-to-processing latency."""
    df = metrics.get("latency", pd.DataFrame())
    if df.empty or "_latency_sec" not in df.columns:
        return _empty_fig("Batch Latency", "No latency data available")

    latencies = df["_latency_sec"].dropna()

    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=latencies, nbinsx=30,
        marker_color="#3498db",
        name="Latency (sec)",
    ))
    fig.add_vline(
        x=latencies.median(), line_dash="dash", line_color="red",
        annotation_text=f"Median: {latencies.median():.1f}s",
    )
    fig.update_layout(
        title="Processing Latency (Ingestion to Silver)",
        xaxis_title="Latency (seconds)",
        yaxis_title="Record Count",
        template="plotly_white",
        height=350,
    )
    return fig


def fig_device_health(metrics: dict) -> go.Figure:
    """Health score distribution with risk-tier coloring."""
    df = metrics.get("device_health", pd.DataFrame())
    if df.empty:
        return _empty_fig("Device Health", "No health data available")

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=["Health Score Distribution", "Risk Tier Breakdown"],
        specs=[[{"type": "histogram"}, {"type": "pie"}]],
    )

    for tier, color in TIER_COLORS.items():
        subset = df[df["risk_tier"] == tier]
        if not subset.empty:
            fig.add_trace(
                go.Histogram(
                    x=subset["health_score"],
                    name=tier.capitalize(),
                    marker_color=color,
                    nbinsx=20,
                ),
                row=1, col=1,
            )

    tier_counts = df["risk_tier"].value_counts()
    fig.add_trace(
        go.Pie(
            labels=[t.capitalize() for t in tier_counts.index],
            values=tier_counts.values,
            marker_colors=[TIER_COLORS.get(t, "#888") for t in tier_counts.index],
            hole=0.4,
        ),
        row=1, col=2,
    )

    fig.update_layout(
        title="Device Health Overview",
        template="plotly_white",
        height=400,
        barmode="stack",
    )
    return fig


def fig_quality_distribution(metrics: dict) -> go.Figure:
    """Quality score distribution across Silver records."""
    df = metrics.get("latency", pd.DataFrame())
    if df.empty or "_quality_score" not in df.columns:
        return _empty_fig("Quality Distribution", "No quality data")

    scores = df["_quality_score"].dropna()
    anomalous = df[df["_is_anomaly"] == True]["_quality_score"].dropna()  # noqa: E712
    clean = df[df["_is_anomaly"] == False]["_quality_score"].dropna()  # noqa: E712

    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=clean, name="Clean", marker_color="#2ecc71", nbinsx=20, opacity=0.7,
    ))
    fig.add_trace(go.Histogram(
        x=anomalous, name="Anomalous", marker_color="#e74c3c", nbinsx=20, opacity=0.7,
    ))
    fig.update_layout(
        title="Data Quality Score Distribution",
        xaxis_title="Quality Score",
        yaxis_title="Record Count",
        barmode="overlay",
        template="plotly_white",
        height=350,
    )
    return fig


def fig_device_summary_heatmap(metrics: dict) -> go.Figure:
    """Heatmap of average temperature by device."""
    df = metrics.get("device_summary", pd.DataFrame())
    if df.empty:
        return _empty_fig("Device Summary", "No summary data")

    top = df.nlargest(20, "event_count")

    fig = go.Figure(go.Bar(
        x=top["device_id"],
        y=top["avg_temperature"],
        marker_color=top["anomaly_rate"].apply(
            lambda r: "#e74c3c" if r > 0 else "#2ecc71"
        ),
        text=top["event_count"].apply(lambda c: f"{c} events"),
        textposition="outside",
    ))
    fig.update_layout(
        title="Top 20 Devices: Avg Temperature (red = has anomalies)",
        xaxis_title="Device",
        yaxis_title="Avg Temperature",
        template="plotly_white",
        height=400,
    )
    return fig


# ── HTML Report Generator ───────────────────────────────────────────────────


def generate_html_report(
    spark: SparkSession, config: dict, output_path: str = "monitoring/report.html",
) -> str:
    """Build and save the complete observability dashboard as HTML."""
    logger.info("Collecting pipeline metrics...")
    metrics = collect_pipeline_metrics(spark, config)

    logger.info("Building charts...")
    figures = [
        fig_throughput(metrics),
        fig_anomaly_spikes(metrics),
        fig_delta_growth(metrics),
        fig_batch_latency(metrics),
        fig_device_health(metrics),
        fig_quality_distribution(metrics),
        fig_device_summary_heatmap(metrics),
    ]

    chart_divs = "\n".join(
        f.to_html(full_html=False, include_plotlyjs=False) for f in figures
    )

    counts = metrics["layer_counts"]
    anomaly_df = metrics.get("anomaly_summary", pd.DataFrame())
    total_anomalies = int(anomaly_df["total_anomaly_count"].sum()) if not anomaly_df.empty else 0
    health_df = metrics.get("device_health", pd.DataFrame())
    avg_health = f"{health_df['health_score'].mean():.2f}" if not health_df.empty else "N/A"
    latency_df = metrics.get("latency", pd.DataFrame())
    med_latency = (
        f"{latency_df['_latency_sec'].median():.1f}s"
        if not latency_df.empty and "_latency_sec" in latency_df.columns
        else "N/A"
    )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    report_html = dedent(f"""\
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Pipeline Observability Dashboard</title>
        <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
        <style>
            * {{ margin: 0; padding: 0; box-sizing: border-box; }}
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                    Roboto, sans-serif; background: #f5f6fa; color: #2c3e50; }}
            .header {{ background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                       color: white; padding: 32px 48px; }}
            .header h1 {{ font-size: 28px; margin-bottom: 4px; }}
            .header p {{ opacity: 0.7; font-size: 14px; }}
            .kpi-row {{ display: flex; gap: 20px; padding: 24px 48px;
                        flex-wrap: wrap; }}
            .kpi {{ background: white; border-radius: 12px; padding: 20px 28px;
                    flex: 1; min-width: 180px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }}
            .kpi .label {{ font-size: 12px; text-transform: uppercase;
                           letter-spacing: 1px; color: #7f8c8d; margin-bottom: 6px; }}
            .kpi .value {{ font-size: 32px; font-weight: 700; }}
            .kpi .value.bronze {{ color: #cd7f32; }}
            .kpi .value.silver {{ color: #808080; }}
            .kpi .value.gold {{ color: #daa520; }}
            .kpi .value.anomaly {{ color: #e74c3c; }}
            .kpi .value.health {{ color: #2ecc71; }}
            .kpi .value.latency {{ color: #3498db; }}
            .charts {{ padding: 12px 48px 48px; }}
            .chart-card {{ background: white; border-radius: 12px;
                           padding: 16px; margin-bottom: 24px;
                           box-shadow: 0 2px 8px rgba(0,0,0,0.06); }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>IoT Telemetry Pipeline - Observability Dashboard</h1>
            <p>Generated {html.escape(now)}</p>
        </div>
        <div class="kpi-row">
            <div class="kpi">
                <div class="label">Bronze Rows</div>
                <div class="value bronze">{counts.get('Bronze', 0):,}</div>
            </div>
            <div class="kpi">
                <div class="label">Silver Rows</div>
                <div class="value silver">{counts.get('Silver', 0):,}</div>
            </div>
            <div class="kpi">
                <div class="label">Gold Rows (total)</div>
                <div class="value gold">{counts.get('Gold', 0):,}</div>
            </div>
            <div class="kpi">
                <div class="label">Total Anomalies</div>
                <div class="value anomaly">{total_anomalies:,}</div>
            </div>
            <div class="kpi">
                <div class="label">Avg Health Score</div>
                <div class="value health">{avg_health}</div>
            </div>
            <div class="kpi">
                <div class="label">Median Latency</div>
                <div class="value latency">{med_latency}</div>
            </div>
        </div>
        <div class="charts">
            {chart_divs}
        </div>
    </body>
    </html>
    """)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report_html, encoding="utf-8")
    logger.info("Dashboard saved to %s", output.resolve())
    return str(output.resolve())


# ── Helpers ──────────────────────────────────────────────────────────────────


def _empty_fig(title: str, message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=message, xref="paper", yref="paper",
        x=0.5, y=0.5, showarrow=False, font_size=16,
    )
    fig.update_layout(title=title, template="plotly_white", height=300)
    return fig
