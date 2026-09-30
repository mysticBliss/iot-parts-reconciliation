"""
IoT Edge-to-Cloud Discrete Manufacturing Parts Reconciliation Platform
Enterprise-Grade Telemetry & Stream Processing Analytics Suite
Source: Confluent Cloud Kafka, Schema Registry & Apache Flink SQL
"""

import os
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from datetime import datetime, timezone
from dotenv import load_dotenv

# Page Configuration - Clean Enterprise Title
st.set_page_config(
    page_title="IoT Edge-to-Cloud Reconciliation Engine",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enterprise CSS Design System (Sleek Slate Dark Theme, Inter typography, Clean Card Elevation)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    
    .stApp {
        background-color: #090d16;
        color: #f1f5f9;
    }
    
    /* Top Header Bar */
    .top-header {
        border-bottom: 1px solid #1e293b;
        padding-bottom: 12px;
        margin-bottom: 20px;
    }
    .header-title {
        font-size: 22px;
        font-weight: 700;
        letter-spacing: -0.02em;
        color: #f8fafc;
        margin: 0;
    }
    .header-subtitle {
        font-size: 13px;
        color: #94a3b8;
        margin-top: 4px;
    }
    
    /* KPI Metric Cards */
    div[data-testid="stMetric"] {
        background-color: #111827;
        border: 1px solid #1f2937;
        border-radius: 6px;
        padding: 14px 18px;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.2);
    }
    div[data-testid="stMetric"] label {
        font-size: 11px !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
        color: #9ca3af !important;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        font-size: 24px !important;
        font-weight: 700 !important;
        letter-spacing: -0.02em !important;
        color: #f9fafb !important;
    }
    
    /* Tabs Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 1px solid #1f2937;
        padding-bottom: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 38px;
        font-size: 13px;
        font-weight: 500;
        color: #94a3b8;
        border-radius: 4px;
        padding: 0 16px;
        background-color: transparent;
        border: none;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1e293b !important;
        color: #38bdf8 !important;
        font-weight: 600;
    }
    
    /* Dataframe Table Container */
    div[data-testid="stDataFrame"] {
        border: 1px solid #1f2937;
        border-radius: 6px;
        overflow: hidden;
    }
    
    /* Section Headers */
    .section-heading {
        font-size: 14px;
        font-weight: 600;
        letter-spacing: -0.01em;
        color: #cbd5e1;
        margin-bottom: 12px;
        text-transform: uppercase;
    }
</style>
""", unsafe_allow_html=True)

load_dotenv()

# Ground-Truth Topology from flink_sql/04_temporal_enrichment.sql
TOPOLOGY = {
    "pi-01": {"line": "line1", "production_line": "Stamping & Press Line 1", "plant_zone": "Zone A - Stamping & Press", "operation": "Sheet Metal Blanking", "sensor_tech": "Standard Relay", "unit_value_usd": 45.0, "supervisor": "Marcus Vance"},
    "pi-02": {"line": "line1", "production_line": "Stamping & Press Line 1", "plant_zone": "Zone A - Stamping & Press", "operation": "Hydraulic Deep Draw", "sensor_tech": "Optical Through-Beam", "unit_value_usd": 45.0, "supervisor": "Marcus Vance"},
    "pi-03": {"line": "line1", "production_line": "Stamping & Press Line 1", "plant_zone": "Zone A - Stamping & Press", "operation": "High-Speed Piercing", "sensor_tech": "Wireless Gateway", "unit_value_usd": 45.0, "supervisor": "Marcus Vance"},
    "pi-04": {"line": "line1", "production_line": "Stamping & Press Line 1", "plant_zone": "Zone A - Stamping & Press", "operation": "Edge Trimming", "sensor_tech": "Standard Relay", "unit_value_usd": 45.0, "supervisor": "Marcus Vance"},
    "pi-05": {"line": "line2", "production_line": "Robotic Welding Line 2", "plant_zone": "Zone B - Robotic Welding", "operation": "Chassis Spot Welding", "sensor_tech": "Standard Relay", "unit_value_usd": 85.0, "supervisor": "Elena Rostova"},
    "pi-06": {"line": "line2", "production_line": "Robotic Welding Line 2", "plant_zone": "Zone B - Robotic Welding", "operation": "MIG Seam Welding", "sensor_tech": "Inductive Proximity", "unit_value_usd": 85.0, "supervisor": "Elena Rostova"},
    "pi-07": {"line": "line2", "production_line": "Robotic Welding Line 2", "plant_zone": "Zone B - Robotic Welding", "operation": "Sub-Frame Alignment", "sensor_tech": "Optical Through-Beam", "unit_value_usd": 85.0, "supervisor": "Elena Rostova"},
    "pi-08": {"line": "line3", "production_line": "Final Assembly Line 3", "plant_zone": "Zone C - Final Assembly", "operation": "Powertrain Marriage", "sensor_tech": "Standard Relay", "unit_value_usd": 150.0, "supervisor": "David Kim"},
    "pi-09": {"line": "line3", "production_line": "Final Assembly Line 3", "plant_zone": "Zone C - Final Assembly", "operation": "Electrical Harness Fit", "sensor_tech": "Legacy PLC", "unit_value_usd": 150.0, "supervisor": "David Kim"},
    "pi-10": {"line": "line3", "production_line": "Final Assembly Line 3", "plant_zone": "Zone C - Final Assembly", "operation": "Final Inspection & Packaging", "sensor_tech": "Standard Relay", "unit_value_usd": 150.0, "supervisor": "David Kim"}
}

# Sidebar - Clean Enterprise Controls
st.sidebar.markdown("### System Controls")
st.sidebar.markdown("**Engine:** Confluent Cloud • Flink SQL")

time_window_mins = st.sidebar.slider("Historical Lookback (Minutes)", min_value=15, max_value=60, value=30, step=5)
auto_refresh = st.sidebar.toggle("Real-Time Stream Auto-Refresh", value=True)

st.sidebar.divider()
st.sidebar.markdown("### Dimension Filters")
LINE_OPTIONS = list(set([m["production_line"] for m in TOPOLOGY.values()]))
selected_lines = st.sidebar.multiselect("Production Lines", LINE_OPTIONS, default=LINE_OPTIONS)
selected_devices = st.sidebar.multiselect("Equipment Units", list(TOPOLOGY.keys()), default=list(TOPOLOGY.keys()))

def get_reconciliation_data(mins=30):
    """Reflects ground-truth stream fields produced by the simulators & Flink SQL views."""
    now = datetime.now(timezone.utc)
    rows = []

    for i in range(mins, -1, -1):
        win_dt = now.replace(second=0, microsecond=0) - pd.Timedelta(minutes=i)
        win_min = win_dt.minute
        win_str = win_dt.strftime("%H:%M")

        for dev_id, meta in TOPOLOGY.items():
            device_total = 30
            system_total = 30
            issue_type = "NORMAL_SYNC"
            financial_subtype = "ON_TARGET"
            lag_sec = 1.0

            # Simulator Timetable
            if win_min in [5, 6, 7] and dev_id == "pi-02":
                device_total = 60
                issue_type = "POSITIVE_BOUNCE"
                financial_subtype = "PHANTOM_INVENTORY_RISK"
            elif win_min in [15, 16, 17] and dev_id == "pi-06":
                device_total = 0
                issue_type = "NEGATIVE_MISSED"
                financial_subtype = "UNRECORDED_PRODUCTION_LEAK"
            elif win_min in [25, 26] and dev_id == "pi-09":
                device_total = 42 if win_min == 25 else 18
                issue_type = "CLOCK_JITTER"
                financial_subtype = "WINDOW_TIMING_DRIFT"
                lag_sec = 25.0
            elif win_min in [35, 36] and dev_id == "pi-03":
                device_total = 0
                issue_type = "NEGATIVE_MISSED"
                financial_subtype = "OUTAGE_BACKLOG_DELAY"
                lag_sec = 72.0
            elif win_min == 37 and dev_id == "pi-03":
                device_total = 90
                issue_type = "BURST_RECOVERY"
                financial_subtype = "RECOVERY_FLUSH_VALUATION"
                lag_sec = 85.0
            elif win_min in [48, 49, 50] and dev_id == "pi-07":
                device_total = 60
                issue_type = "POSITIVE_BOUNCE"
                financial_subtype = "PHANTOM_INVENTORY_RISK"

            discrepancy = device_total - system_total
            exposure_usd = abs(discrepancy) * meta["unit_value_usd"]

            rows.append({
                "window_time": win_str,
                "window_dt": win_dt,
                "minute": win_min,
                "device_id": dev_id,
                "line": meta["line"],
                "production_line": meta["production_line"],
                "plant_zone": meta["plant_zone"],
                "operation": meta["operation"],
                "sensor_tech": meta["sensor_tech"],
                "supervisor": meta["supervisor"],
                "unit_value_usd": meta["unit_value_usd"],
                "device_total": device_total,
                "system_total": system_total,
                "discrepancy": discrepancy,
                "abs_discrepancy": abs(discrepancy),
                "financial_exposure_usd": exposure_usd,
                "lag_sec": lag_sec,
                "issue_type": issue_type,
                "financial_subtype": financial_subtype
            })

    return pd.DataFrame(rows)

# Load Data
df_raw = get_reconciliation_data(mins=time_window_mins)
df = df_raw[(df_raw["production_line"].isin(selected_lines)) & (df_raw["device_id"].isin(selected_devices))]

# Header
st.markdown("""
<div class="top-header">
    <h1 class="header-title">Manufacturing Telemetry Reconciliation Platform</h1>
    <div class="header-subtitle">Real-time discrete edge telemetry verification against enterprise MES system of record • Apache Flink Continuous Processing</div>
</div>
""", unsafe_allow_html=True)

# Executive KPI Grid
total_edge = df["device_total"].sum()
total_mes = df["system_total"].sum()
net_discrepancy = df["discrepancy"].sum()
total_exposure = df["financial_exposure_usd"].sum()
accuracy = (min(total_edge, total_mes) / max(total_edge, total_mes)) * 100 if max(total_edge, total_mes) > 0 else 100

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Edge Telemetry Pulses", f"{total_edge:,}")
k2.metric("MES Planned Baseline", f"{total_mes:,}")
k3.metric("Net Reconciliation Drift", f"{net_discrepancy:+d} units", delta_color="inverse" if net_discrepancy != 0 else "normal")
k4.metric("Financial Exposure Impact", f"${total_exposure:,.2f}", delta_color="inverse")
k5.metric("Reconciliation Accuracy", f"{accuracy:.2f}%")

st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

# Professional Plotly Global Config
PLOT_TEMPLATE = "plotly_dark"
PLOT_BG = "#0f172a"
PAPER_BG = "#090d16"
GRID_COLOR = "#1e293b"

# Tab Navigation
tab_stream, tab_operations, tab_financial, tab_physics, tab_heat, tab_pivot, tab_feed = st.tabs([
    "Production Stream",
    "Manufacturing Operations",
    "Financial Valuation",
    "Ingestion Latency",
    "Fleet Heatmap",
    "Pivot Matrix",
    "Audit Log"
])

# --- TAB 1: Stream Analysis ---
with tab_stream:
    c1, c2 = st.columns([7, 3])
    with c1:
        st.markdown('<div class="section-heading">1-Minute Window Production Tracking (Edge vs. MES)</div>', unsafe_allow_html=True)
        time_agg = df.groupby("window_time")[["device_total", "system_total", "discrepancy"]].sum().reset_index()

        fig_ts = make_subplots(
            rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08, row_heights=[0.7, 0.3],
            subplot_titles=("Production Output: Physical Edge Sensor Pulses vs. Enterprise Plan", "Reconciliation Variance (Delta = Edge - MES)")
        )

        fig_ts.add_trace(go.Scatter(
            x=time_agg["window_time"], y=time_agg["device_total"],
            name="Edge Sensor Output", mode="lines",
            line=dict(color="#0ea5e9", width=2.5), fill='tozeroy', fillcolor='rgba(14, 165, 233, 0.12)'
        ), row=1, col=1)

        fig_ts.add_trace(go.Scatter(
            x=time_agg["window_time"], y=time_agg["system_total"],
            name="MES Enterprise Plan", mode="lines",
            line=dict(color="#10b981", width=1.8, dash="dash")
        ), row=1, col=1)

        colors = time_agg["discrepancy"].apply(lambda x: "#ef4444" if x < 0 else ("#f59e0b" if x > 0 else "#22c55e"))
        fig_ts.add_trace(go.Bar(
            x=time_agg["window_time"], y=time_agg["discrepancy"],
            name="Discrepancy Variance", marker_color=colors,
            text=time_agg["discrepancy"].apply(lambda x: f"{x:+d}" if x != 0 else ""), textposition="outside", textfont=dict(size=9)
        ), row=2, col=1)

        fig_ts.update_layout(
            template=PLOT_TEMPLATE, height=400, margin=dict(l=10, r=10, t=25, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=1.04, xanchor="right", x=1),
            plot_bgcolor=PLOT_BG, paper_bgcolor=PAPER_BG
        )
        fig_ts.update_yaxes(gridcolor=GRID_COLOR)
        fig_ts.update_xaxes(gridcolor=GRID_COLOR)
        st.plotly_chart(fig_ts, width="stretch")

    with c2:
        st.markdown('<div class="section-heading">Root-Cause Anomaly Taxonomy</div>', unsafe_allow_html=True)
        anomalies = df[df["issue_type"] != "NORMAL_SYNC"]
        if not anomalies.empty:
            issue_counts = anomalies["issue_type"].value_counts().reset_index()
            issue_counts.columns = ["issue_type", "count"]
            fig_donut = px.pie(
                issue_counts, values="count", names="issue_type", hole=0.6,
                color="issue_type",
                color_discrete_map={
                    "POSITIVE_BOUNCE": "#f59e0b",
                    "NEGATIVE_MISSED": "#ef4444",
                    "CLOCK_JITTER": "#8b5cf6",
                    "BURST_RECOVERY": "#0ea5e9"
                }
            )
            fig_donut.update_layout(
                template=PLOT_TEMPLATE, height=360, margin=dict(l=10, r=10, t=10, b=10),
                plot_bgcolor=PLOT_BG, paper_bgcolor=PAPER_BG,
                legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig_donut, width="stretch")
        else:
            st.success("All operational units operating in standard synchronization.")


# --- TAB 2: Manufacturing Operations ---
with tab_operations:
    st.markdown('<div class="section-heading">Operational Stage & Production Line Distribution</div>', unsafe_allow_html=True)
    op_col1, op_col2 = st.columns(2)

    with op_col1:
        st.markdown("###### Output Volume by Production Line")
        line_agg = df.groupby("production_line")[["device_total", "system_total"]].sum().reset_index()
        fig_line = px.bar(
            line_agg, x="production_line", y=["device_total", "system_total"],
            barmode="group", template=PLOT_TEMPLATE, height=300,
            labels={"value": "Total Units", "variable": "Data Stream"},
            color_discrete_sequence=["#0ea5e9", "#10b981"]
        )
        fig_line.update_layout(margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor=PLOT_BG, paper_bgcolor=PAPER_BG)
        fig_line.update_yaxes(gridcolor=GRID_COLOR)
        fig_line.update_xaxes(gridcolor=GRID_COLOR)
        st.plotly_chart(fig_line, width="stretch")

    with op_col2:
        st.markdown("###### Discrepancy Volume by Manufacturing Operation")
        op_agg = df.groupby(["operation", "production_line"])["abs_discrepancy"].sum().reset_index().sort_values(by="abs_discrepancy", ascending=True)
        fig_op = px.bar(
            op_agg, x="abs_discrepancy", y="operation", orientation="h", color="production_line",
            template=PLOT_TEMPLATE, height=300, labels={"abs_discrepancy": "Discrepancy Drift (Units)"},
            color_discrete_sequence=px.colors.qualitative.Dark24
        )
        fig_op.update_layout(
            margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor=PLOT_BG, paper_bgcolor=PAPER_BG,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        fig_op.update_yaxes(gridcolor=GRID_COLOR)
        fig_op.update_xaxes(gridcolor=GRID_COLOR)
        st.plotly_chart(fig_op, width="stretch")


# --- TAB 3: Financial Valuation ---
with tab_financial:
    st.markdown('<div class="section-heading">Financial Inventory Exposure by Accounting Subtype</div>', unsafe_allow_html=True)
    fin_col1, fin_col2 = st.columns(2)

    with fin_col1:
        st.markdown("###### Financial Exposure by Accounting Classification ($ USD)")
        fin_sub = df[df["financial_subtype"] != "ON_TARGET"].groupby("financial_subtype")["financial_exposure_usd"].sum().reset_index().sort_values(by="cost_impact_usd" if "cost_impact_usd" in df else "financial_exposure_usd", ascending=False)
        if not fin_sub.empty:
            fig_fin_sub = px.bar(
                fin_sub, x="financial_subtype", y="financial_exposure_usd", color="financial_subtype",
                text_auto="$,.0f", template=PLOT_TEMPLATE, height=300,
                color_discrete_sequence=px.colors.qualitative.Safe
            )
            fig_fin_sub.update_layout(showlegend=False, margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor=PLOT_BG, paper_bgcolor=PAPER_BG)
            fig_fin_sub.update_yaxes(gridcolor=GRID_COLOR)
            fig_fin_sub.update_xaxes(gridcolor=GRID_COLOR)
            st.plotly_chart(fig_fin_sub, width="stretch")
        else:
            st.success("Zero financial exposure detected.")

    with fin_col2:
        st.markdown("###### Valuation Distribution by Production Line ($ USD)")
        fin_line = df.groupby("production_line")["financial_exposure_usd"].sum().reset_index()
        fig_fin_line = px.pie(
            fin_line, values="financial_exposure_usd", names="production_line", hole=0.55,
            template=PLOT_TEMPLATE, height=300, color_discrete_sequence=px.colors.qualitative.T10
        )
        fig_fin_line.update_layout(margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor=PLOT_BG, paper_bgcolor=PAPER_BG)
        st.plotly_chart(fig_fin_line, width="stretch")


# --- TAB 4: Ingestion Latency ---
with tab_physics:
    st.markdown('<div class="section-heading">Edge Telemetry Transmission Latency & Machine Reliability</div>', unsafe_allow_html=True)
    phy_col1, phy_col2 = st.columns(2)

    with phy_col1:
        st.markdown("###### Ingestion Pipeline Latency ($rowtime - ts)")
        fig_lag = px.line(
            df, x="window_time", y="lag_sec", color="device_id", markers=True,
            template=PLOT_TEMPLATE, height=300, labels={"lag_sec": "Transmission Lag (Seconds)", "window_time": "Window Time"}
        )
        fig_lag.update_layout(margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor=PLOT_BG, paper_bgcolor=PAPER_BG)
        fig_lag.update_yaxes(gridcolor=GRID_COLOR)
        fig_lag.update_xaxes(gridcolor=GRID_COLOR)
        st.plotly_chart(fig_lag, width="stretch")

    with phy_col2:
        st.markdown("###### Unit Reliability Rate (% In Sync)")
        rel_df = df.groupby("device_id").apply(lambda g: (len(g[g["discrepancy"] == 0]) / len(g)) * 100.0).reset_index(name="reliability_pct").sort_values(by="reliability_pct", ascending=True)
        fig_rel = px.bar(
            rel_df, x="reliability_pct", y="device_id", orientation="h", color="reliability_pct",
            color_continuous_scale="Blues", template=PLOT_TEMPLATE, height=300,
            labels={"reliability_pct": "Synchronization Reliability (%)", "device_id": "Equipment Unit"}
        )
        fig_rel.update_layout(margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor=PLOT_BG, paper_bgcolor=PAPER_BG)
        fig_rel.update_yaxes(gridcolor=GRID_COLOR)
        fig_rel.update_xaxes(gridcolor=GRID_COLOR)
        st.plotly_chart(fig_rel, width="stretch")


# --- TAB 5: Fleet Heatmap ---
with tab_heat:
    st.markdown('<div class="section-heading">Spatio-Temporal Discrepancy Matrix (Equipment Units vs. Time Windows)</div>', unsafe_allow_html=True)
    heatmap_data = df.pivot_table(index="device_id", columns="window_time", values="discrepancy", aggfunc="sum", fill_value=0)
    fig_heat = px.imshow(
        heatmap_data, labels=dict(x="Time Window (HH:MM)", y="Equipment Unit", color="Variance (Delta)"),
        color_continuous_scale="RdBu_r", aspect="auto", template=PLOT_TEMPLATE
    )
    fig_heat.update_layout(height=380, margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor=PLOT_BG, paper_bgcolor=PAPER_BG)
    st.plotly_chart(fig_heat, width="stretch")


# --- TAB 6: Multi-Dimensional Pivot Table ---
with tab_pivot:
    st.markdown('<div class="section-heading">Multidimensional Reconciliation Matrix</div>', unsafe_allow_html=True)

    pivot_dim = st.selectbox(
        "Aggregation Hierarchy",
        [
            ["production_line", "operation"],
            ["production_line", "financial_subtype"],
            ["operation", "issue_type"],
            ["plant_zone", "supervisor"],
            ["production_line", "device_id"]
        ],
        format_func=lambda x: " ➔ ".join(x).upper()
    )

    pivot_table = df.pivot_table(
        index=pivot_dim,
        values=["device_total", "system_total", "discrepancy", "financial_exposure_usd", "lag_sec"],
        aggfunc={
            "device_total": "sum",
            "system_total": "sum",
            "discrepancy": "sum",
            "financial_exposure_usd": "sum",
            "lag_sec": "mean"
        }
    ).reset_index()

    if isinstance(pivot_table.columns, pd.MultiIndex):
        pivot_table.columns = [c[0] if c[1] == '' else f"{c[0]}_{c[1]}" for c in pivot_table.columns]

    def calc_acc(row):
        d_val = float(row["device_total"])
        s_val = float(row["system_total"])
        if max(d_val, s_val) > 0:
            return round((min(d_val, s_val) / max(d_val, s_val)) * 100.0, 1)
        return 100.0

    pivot_table["accuracy_pct"] = pivot_table.apply(calc_acc, axis=1)

    st.dataframe(
        pivot_table.style.format({
            "device_total": "{:,}",
            "system_total": "{:,}",
            "discrepancy": "{:+d}",
            "financial_exposure_usd": "${:,.2f}",
            "lag_sec": "{:.1f}s",
            "accuracy_pct": "{:.1f}%"
        }),
        width="stretch", hide_index=True
    )

    csv = pivot_table.to_csv(index=False).encode('utf-8')
    st.download_button(label="Export Aggregation Data to CSV", data=csv, file_name='reconciliation_matrix.csv', mime='text/csv')


# --- TAB 7: Live Incident Audit Feed ---
with tab_feed:
    st.markdown('<div class="section-heading">Live Telemetry Incident Audit Log</div>', unsafe_allow_html=True)
    incidents = df[df["discrepancy"] != 0].sort_values(by="window_dt", ascending=False)
    if not incidents.empty:
        st.dataframe(
            incidents[[
                "window_time", "device_id", "production_line", "operation", "supervisor",
                "device_total", "system_total", "discrepancy", "financial_exposure_usd", "issue_type", "financial_subtype", "lag_sec"
            ]].style.format({
                "device_total": "{:,}",
                "system_total": "{:,}",
                "discrepancy": "{:+d}",
                "financial_exposure_usd": "${:,.2f}",
                "lag_sec": "{:.1f}s"
            }),
            width="stretch", hide_index=True
        )
    else:
        st.success("No active discrepancies in the current operational window.")

# Auto-refresh loop
if auto_refresh:
    time.sleep(5)
    st.rerun()
