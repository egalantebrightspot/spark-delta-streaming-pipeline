"""Live monitoring dashboard served via Plotly Dash.

Provides a real-time view of pipeline health with three core panels:

  1. Event Throughput   — rows per layer with Bronze/Silver/Gold funnel
  2. Anomaly Counts     — per-device stacked bar by sensor type
  3. Device Health      — score histogram + risk-tier pie chart

The dashboard auto-refreshes on a configurable interval (default 30 s)
by re-querying the Delta tables.  All Spark operations use small
collected result sets — no Python workers required.

Usage:
    python -m monitoring.live_dashboard            # default config
    python -m monitoring.live_dashboard --port 8050 # custom port
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

try:
    from dash import Dash, html, dcc
    from dash.dependencies import Input, Output
except ImportError:
    raise SystemExit(
        "dash is required for the live dashboard.  Install it with:\n"
        "  pip install dash"
    )

from pipeline.common.utils import load_config, get_spark_session
from pipeline.common.logging_config import get_logger
from pipeline.gold_aggregations.gold_aggregations_job import gold_table_path

logger = get_logger("monitoring.live_dashboard")

TIER_COLORS = {"healthy": "#2ecc71", "warning": "#f39c12", "critical": "#e74c3c"}
LAYER_COLORS = {"Bronze": "#cd7f32", "Silver": "#c0c0c0", "Gold": "#ffd700"}
REFRESH_INTERVAL_MS = 30_000


# ── Data Collection ──────────────────────────────────────────────────────────


def _safe_count(spark, path: str) -> int:
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return 0


def collect_layer_counts(spark, config: dict) -> dict[str, int]:
    paths = config["paths"]
    counts = {
        "Bronze": _safe_count(spark, paths["delta_bronze"]),
        "Silver": _safe_count(spark, paths["delta_silver"]),
    }
    gold_total = 0
    for t in ["device_summary", "anomaly_summary", "device_health", "ml_features"]:
        gold_total += _safe_count(spark, gold_table_path(config, t))
    counts["Gold"] = gold_total
    return counts


def collect_anomaly_data(spark, config: dict) -> pd.DataFrame:
    try:
        return spark.read.format("delta").load(
            gold_table_path(config, "anomaly_summary"),
        ).toPandas()
    except Exception:
        return pd.DataFrame()


def collect_health_data(spark, config: dict) -> pd.DataFrame:
    try:
        return spark.read.format("delta").load(
            gold_table_path(config, "device_health"),
        ).toPandas()
    except Exception:
        return pd.DataFrame()


# ── Chart Builders ───────────────────────────────────────────────────────────


def build_throughput_figure(layer_counts: dict[str, int]) -> go.Figure:
    layers = list(layer_counts.keys())
    values = list(layer_counts.values())
    colors = [LAYER_COLORS.get(l, "#888") for l in layers]

    fig = go.Figure(go.Bar(
        x=layers, y=values,
        marker_color=colors,
        text=[f"{v:,}" for v in values],
        textposition="outside",
    ))
    fig.update_layout(
        title="Event Throughput: Rows per Layer",
        yaxis_title="Row Count",
        template="plotly_white",
        height=370,
        margin=dict(t=50, b=40),
    )
    return fig


def build_anomaly_figure(anomaly_df: pd.DataFrame) -> go.Figure:
    if anomaly_df.empty:
        return _empty_fig("Anomaly Counts", "No anomaly data — run Gold job first")

    agg = anomaly_df.groupby("device_id").agg({
        "temp_anomaly_count": "sum",
        "humidity_anomaly_count": "sum",
        "pressure_anomaly_count": "sum",
        "total_anomaly_count": "sum",
    }).reset_index()
    agg = agg[agg["total_anomaly_count"] > 0].sort_values(
        "total_anomaly_count", ascending=True,
    )

    if agg.empty:
        return _empty_fig("Anomaly Counts", "No anomalies detected in fleet")

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
        title="Anomaly Counts by Device and Sensor Type",
        xaxis_title="Anomaly Count",
        barmode="stack",
        template="plotly_white",
        height=max(350, len(agg) * 28 + 100),
        margin=dict(t=50, b=40),
    )
    return fig


def build_health_figure(health_df: pd.DataFrame) -> go.Figure:
    if health_df.empty:
        return _empty_fig("Device Health", "No health data — run Gold job first")

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=["Health Score Distribution", "Risk Tier Breakdown"],
        specs=[[{"type": "histogram"}, {"type": "pie"}]],
    )

    for tier, color in TIER_COLORS.items():
        subset = health_df[health_df["risk_tier"] == tier]
        if not subset.empty:
            fig.add_trace(go.Histogram(
                x=subset["health_score"],
                name=tier.capitalize(),
                marker_color=color,
                nbinsx=20,
            ), row=1, col=1)

    tier_counts = health_df["risk_tier"].value_counts()
    fig.add_trace(go.Pie(
        labels=[t.capitalize() for t in tier_counts.index],
        values=tier_counts.values,
        marker_colors=[TIER_COLORS.get(t, "#888") for t in tier_counts.index],
        hole=0.4,
    ), row=1, col=2)

    fig.update_layout(
        title="Device Health Distribution",
        template="plotly_white",
        height=400,
        barmode="stack",
        margin=dict(t=50, b=40),
    )
    return fig


def _empty_fig(title: str, message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=message, xref="paper", yref="paper",
        x=0.5, y=0.5, showarrow=False, font_size=16,
    )
    fig.update_layout(title=title, template="plotly_white", height=300)
    return fig


# ── Dash Application ────────────────────────────────────────────────────────


def create_app(spark, config: dict, refresh_ms: int = REFRESH_INTERVAL_MS) -> Dash:
    app = Dash(
        __name__,
        title="IoT Pipeline Monitor",
        update_title="Refreshing...",
    )

    app.layout = html.Div(
        style={
            "fontFamily": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
            "backgroundColor": "#f5f6fa",
            "minHeight": "100vh",
        },
        children=[
            html.Div(
                style={
                    "background": "linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)",
                    "color": "white",
                    "padding": "28px 48px",
                },
                children=[
                    html.H1(
                        "IoT Telemetry Pipeline — Live Monitor",
                        style={"marginBottom": "4px", "fontSize": "26px"},
                    ),
                    html.P(
                        f"Auto-refreshes every {refresh_ms // 1000}s",
                        style={"opacity": "0.7", "fontSize": "13px"},
                    ),
                ],
            ),

            html.Div(id="kpi-row", style={
                "display": "flex", "gap": "20px", "padding": "24px 48px",
                "flexWrap": "wrap",
            }),

            html.Div(style={"padding": "0 48px 48px"}, children=[
                html.Div(id="throughput-card", style=_card_style()),
                html.Div(id="anomaly-card", style=_card_style()),
                html.Div(id="health-card", style=_card_style()),
            ]),

            dcc.Interval(id="refresh-timer", interval=refresh_ms, n_intervals=0),
        ],
    )

    @app.callback(
        [
            Output("kpi-row", "children"),
            Output("throughput-card", "children"),
            Output("anomaly-card", "children"),
            Output("health-card", "children"),
        ],
        Input("refresh-timer", "n_intervals"),
    )
    def refresh(_n):
        counts = collect_layer_counts(spark, config)
        anomaly_df = collect_anomaly_data(spark, config)
        health_df = collect_health_data(spark, config)

        total_anomalies = (
            int(anomaly_df["total_anomaly_count"].sum())
            if not anomaly_df.empty else 0
        )
        avg_health = (
            f"{health_df['health_score'].mean():.2f}"
            if not health_df.empty else "N/A"
        )
        critical_count = (
            int((health_df["risk_tier"] == "critical").sum())
            if not health_df.empty else 0
        )

        kpis = [
            _kpi_card("Bronze Rows", f"{counts.get('Bronze', 0):,}", "#cd7f32"),
            _kpi_card("Silver Rows", f"{counts.get('Silver', 0):,}", "#808080"),
            _kpi_card("Gold Rows", f"{counts.get('Gold', 0):,}", "#daa520"),
            _kpi_card("Anomalies", f"{total_anomalies:,}", "#e74c3c"),
            _kpi_card("Avg Health", avg_health, "#2ecc71"),
            _kpi_card("Critical Devices", str(critical_count), "#e74c3c"),
        ]

        throughput_fig = build_throughput_figure(counts)
        anomaly_fig = build_anomaly_figure(anomaly_df)
        health_fig = build_health_figure(health_df)

        return (
            kpis,
            dcc.Graph(figure=throughput_fig),
            dcc.Graph(figure=anomaly_fig),
            dcc.Graph(figure=health_fig),
        )

    return app


def _card_style() -> dict:
    return {
        "background": "white",
        "borderRadius": "12px",
        "padding": "16px",
        "marginBottom": "24px",
        "boxShadow": "0 2px 8px rgba(0,0,0,0.06)",
    }


def _kpi_card(label: str, value: str, color: str) -> html.Div:
    return html.Div(
        style={
            "background": "white",
            "borderRadius": "12px",
            "padding": "18px 26px",
            "flex": "1",
            "minWidth": "160px",
            "boxShadow": "0 2px 8px rgba(0,0,0,0.06)",
        },
        children=[
            html.Div(
                label,
                style={
                    "fontSize": "11px",
                    "textTransform": "uppercase",
                    "letterSpacing": "1px",
                    "color": "#7f8c8d",
                    "marginBottom": "6px",
                },
            ),
            html.Div(
                value,
                style={"fontSize": "30px", "fontWeight": "700", "color": color},
            ),
        ],
    )


# ── CLI Entry Point ─────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Live pipeline monitoring dashboard")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--refresh", type=int, default=30, help="Refresh interval (seconds)")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    config = load_config()
    spark = get_spark_session(config)

    logger.info("Starting live dashboard on port %d (refresh=%ds)", args.port, args.refresh)
    app = create_app(spark, config, refresh_ms=args.refresh * 1000)
    app.run(host="0.0.0.0", port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
