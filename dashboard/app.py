from pathlib import Path
from typing import Any, Dict

import altair as alt
import os
import pandas as pd
import streamlit as st
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from streamlit_autorefresh import st_autorefresh

ROOT_DIR = Path(__file__).resolve().parents[1]

PALETTE = {
    "bg": "#0f172a",
    "panel": "#111c31",
    "panel_soft": "#16243d",
    "ink": "#e5edf8",
    "muted": "#95a7c2",
    "border": "#243552",
    "blue": "#4ea1ff",
    "blue_dark": "#8bc4ff",
    "teal": "#18b7a6",
    "teal_dark": "#6fe0d5",
    "amber": "#f0b24a",
    "danger": "#f87171",
    "warn": "#facc15",
}


def load_config() -> Dict[str, Any]:
    config_path = Path(os.getenv("RTA_CONFIG_PATH", ROOT_DIR / "config" / "config.yaml"))
    with config_path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    config["postgres"]["host"] = os.getenv("RTA_POSTGRES_HOST", config["postgres"]["host"])
    config["postgres"]["port"] = int(os.getenv("RTA_POSTGRES_PORT", config["postgres"]["port"]))
    config["postgres"]["database"] = os.getenv("RTA_POSTGRES_DB", config["postgres"]["database"])
    config["postgres"]["user"] = os.getenv("RTA_POSTGRES_USER", config["postgres"]["user"])
    config["postgres"]["password"] = os.getenv("RTA_POSTGRES_PASSWORD", config["postgres"]["password"])
    return config


@st.cache_resource(show_spinner=False)
def get_engine(config: Dict[str, Any]):
    db_config = config["postgres"]
    connection_url = (
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    return create_engine(connection_url, pool_pre_ping=True)



def load_metrics(engine) -> pd.DataFrame:
    query = text(
        """
        SELECT
            timestamp,
            total_revenue,
            active_users,
            event_count,
            anomaly_detected
        FROM analytics_metrics
        ORDER BY timestamp DESC
        LIMIT 180
        """
    )
    with engine.connect() as connection:
        frame = pd.read_sql(query, connection)
    if frame.empty:
        return frame
    frame["timestamp"] = pd.to_datetime(frame["timestamp"])
    return frame.sort_values("timestamp")



def load_dead_letter_events(engine) -> pd.DataFrame:
    query = text(
        """
        SELECT
            created_at,
            kafka_timestamp,
            invalid_reason,
            raw_payload
        FROM dead_letter_events
        ORDER BY created_at DESC
        LIMIT 100
        """
    )
    try:
        with engine.connect() as connection:
            frame = pd.read_sql(query, connection)
    except ProgrammingError:
        return pd.DataFrame(columns=["created_at", "kafka_timestamp", "invalid_reason", "raw_payload"])

    if frame.empty:
        return frame

    frame["created_at"] = pd.to_datetime(frame["created_at"])
    frame["kafka_timestamp"] = pd.to_datetime(frame["kafka_timestamp"])
    return frame



def prepare_display_metrics(metrics: pd.DataFrame, points: int) -> pd.DataFrame:
    if metrics.empty:
        return metrics

    trimmed = metrics.tail(points).copy()
    trimmed["timestamp"] = trimmed["timestamp"].dt.floor("min")
    trimmed = trimmed.drop_duplicates(subset=["timestamp"], keep="last").set_index("timestamp")

    full_index = pd.date_range(start=trimmed.index.min(), end=trimmed.index.max(), freq="min")
    display = trimmed.reindex(full_index)
    display.index.name = "timestamp"
    display = display.reset_index()

    display["total_revenue"] = display["total_revenue"].fillna(0.0)
    display["active_users"] = display["active_users"].fillna(0).astype(int)
    display["event_count"] = display["event_count"].fillna(0).astype(int)
    display["anomaly_detected"] = display["anomaly_detected"].where(display["anomaly_detected"].notna(), False).astype(bool)
    display["revenue_rolling"] = display["total_revenue"].rolling(window=3, min_periods=1).mean()
    display["users_rolling"] = display["active_users"].rolling(window=3, min_periods=1).mean()
    display["event_rate_label"] = display["timestamp"].dt.strftime("%H:%M")
    return display



def summarize_dead_letters(dead_letters: pd.DataFrame) -> pd.DataFrame:
    if dead_letters.empty:
        return pd.DataFrame(columns=["invalid_reason", "invalid_count"])
    return (
        dead_letters.groupby("invalid_reason", as_index=False)
        .size()
        .rename(columns={"size": "invalid_count"})
        .sort_values(["invalid_count", "invalid_reason"], ascending=[False, True])
    )



def format_count(value: int) -> str:
    return f"{int(value):,}"



def format_duration(seconds: int) -> str:
    seconds = max(int(seconds), 0)
    hours, remainder = divmod(seconds, 3600)
    minutes, remaining_seconds = divmod(remainder, 60)

    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {remaining_seconds}s"
    return f"{remaining_seconds}s"



def truncate_payload(value: str, limit: int = 120) -> str:
    if value is None:
        return ""
    value = str(value)
    if len(value) <= limit:
        return value
    return f"{value[:limit - 3]}..."



def build_status(freshness_seconds: int, refresh_seconds: int) -> Dict[str, str]:
    if freshness_seconds <= max(refresh_seconds * 3, 20):
        return {
            "label": "Live",
            "footnote": f"Lag {format_duration(freshness_seconds)}",
            "tone": PALETTE["teal_dark"],
        }
    if freshness_seconds <= 900:
        return {
            "label": "Delayed",
            "footnote": f"Lag {format_duration(freshness_seconds)}",
            "tone": PALETTE["warn"],
        }
    return {
        "label": "Stale",
        "footnote": f"Lag {format_duration(freshness_seconds)}",
        "tone": PALETTE["danger"],
    }



def apply_styles() -> None:
    st.markdown(
        f"""
        <style>
        .stApp {{
            background:
                radial-gradient(circle at top right, rgba(78, 161, 255, 0.12), transparent 24%),
                radial-gradient(circle at top left, rgba(24, 183, 166, 0.10), transparent 22%),
                linear-gradient(180deg, {PALETTE['bg']} 0%, #0b1324 100%);
        }}
        .block-container {{
            max-width: 1500px;
            padding-top: 1.4rem;
            padding-bottom: 2rem;
        }}
        .hero {{
            padding: 1.35rem 1.5rem;
            background: linear-gradient(180deg, {PALETTE['panel_soft']} 0%, {PALETTE['panel']} 100%);
            border: 1px solid {PALETTE['border']};
            border-radius: 18px;
            box-shadow: 0 12px 26px rgba(0, 0, 0, 0.18);
            margin-bottom: 0.9rem;
        }}
        .hero-title {{
            color: {PALETTE['ink']};
            font-size: 2.05rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            margin-bottom: 0.28rem;
        }}
        .hero-subtitle {{
            color: {PALETTE['muted']};
            font-size: 0.95rem;
            line-height: 1.55;
            max-width: 1050px;
        }}
        .metric-card {{
            background: {PALETTE['panel']};
            border: 1px solid {PALETTE['border']};
            border-radius: 16px;
            padding: 0.95rem 1rem;
            min-height: 112px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }}
        .metric-label {{
            color: {PALETTE['muted']};
            font-size: 0.76rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 0.45rem;
        }}
        .metric-value {{
            color: {PALETTE['ink']};
            font-size: 1.95rem;
            font-weight: 800;
            line-height: 1.1;
        }}
        .metric-footnote {{
            color: {PALETTE['muted']};
            font-size: 0.82rem;
            margin-top: 0.55rem;
            min-height: 2.4em;
        }}
        .section-title {{
            color: {PALETTE['ink']};
            font-size: 1.02rem;
            font-weight: 800;
            margin: 1rem 0 0.65rem 0;
        }}
        .notes-card {{
            background: {PALETTE['panel']};
            border: 1px solid {PALETTE['border']};
            border-radius: 16px;
            padding: 1rem 1.05rem;
            color: {PALETTE['ink']};
            line-height: 1.65;
        }}
        .stDataFrame {{
            border: 1px solid {PALETTE['border']};
            border-radius: 16px;
            overflow: hidden;
        }}
        code {{
            color: {PALETTE['blue_dark']};
            background: rgba(78, 161, 255, 0.12);
            padding: 0.1rem 0.3rem;
            border-radius: 5px;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )



def render_metric_card(label: str, value: str, footnote: str) -> None:
    st.markdown(
        f"""
        <div class="metric-card">
            <div>
                <div class="metric-label">{label}</div>
                <div class="metric-value">{value}</div>
            </div>
            <div class="metric-footnote">{footnote}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )



def build_revenue_chart(frame: pd.DataFrame, threshold: float) -> alt.Chart:
    base = alt.Chart(frame).encode(x=alt.X("timestamp:T", title="Window"))
    bars = base.mark_bar(color=PALETTE["blue"], opacity=0.78, cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
        y=alt.Y("total_revenue:Q", title="Revenue")
    )
    trend = base.mark_line(color=PALETTE["blue_dark"], strokeWidth=2.6, point=True).encode(y="revenue_rolling:Q")
    threshold_line = alt.Chart(pd.DataFrame({"threshold": [threshold]})).mark_rule(color=PALETTE["danger"], strokeDash=[5, 5]).encode(y="threshold:Q")
    return (bars + trend + threshold_line).properties(height=310).configure_view(stroke=None)



def build_users_chart(frame: pd.DataFrame) -> alt.Chart:
    base = alt.Chart(frame).encode(x=alt.X("timestamp:T", title="Window"))
    area = base.mark_area(color=PALETTE["teal"], opacity=0.24).encode(y=alt.Y("active_users:Q", title="Approx. Active Users"))
    trend = base.mark_line(color=PALETTE["teal_dark"], strokeWidth=2.6, point=True).encode(y="users_rolling:Q")
    return (area + trend).properties(height=310).configure_view(stroke=None)



def build_events_chart(frame: pd.DataFrame) -> alt.Chart:
    trimmed = frame.tail(12)
    return (
        alt.Chart(trimmed)
        .mark_bar(color=PALETTE["amber"], cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("event_rate_label:N", title="Minute", sort=None),
            y=alt.Y("event_count:Q", title="Events"),
            tooltip=["timestamp:T", "event_count:Q", "total_revenue:Q", "active_users:Q"],
        )
        .properties(height=285)
        .configure_view(stroke=None)
    )



def build_dead_letter_chart(frame: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(frame)
        .mark_bar(color=PALETTE["danger"], cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            y=alt.Y("invalid_reason:N", title="Invalid Reason", sort="-x"),
            x=alt.X("invalid_count:Q", title="Dead-Letter Events"),
            tooltip=["invalid_reason:N", "invalid_count:Q"],
        )
        .properties(height=285)
        .configure_view(stroke=None)
    )



def render_dashboard() -> None:
    config = load_config()
    refresh_seconds = int(config["dashboard"]["refresh_seconds"])
    threshold = float(config["anomaly_detection"]["revenue_spike_threshold"])

    st.set_page_config(page_title="Real-Time Analytics", layout="wide")
    st_autorefresh(interval=refresh_seconds * 1000, key="analytics_refresh")
    apply_styles()

    st.markdown(
        """
        <div class="hero">
            <div class="hero-title">Real-Time Analytics Pipeline</div>
            <div class="hero-subtitle">
                Operational dashboard for a streaming analytics stack using Kafka, PostgreSQL, and Streamlit.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    engine = get_engine(config)
    metrics = load_metrics(engine)
    dead_letters = load_dead_letter_events(engine)

    if metrics.empty:
        st.info("No aggregated metrics yet. Wait for the producer and Spark job to populate PostgreSQL.")
        return

    latest = metrics.iloc[-1]
    prev = metrics.iloc[-2] if len(metrics) > 1 else latest
    freshness_delta = pd.Timestamp.utcnow().tz_localize(None) - latest["timestamp"]
    freshness_seconds = max(int(freshness_delta.total_seconds()), 0)
    status = build_status(freshness_seconds, refresh_seconds)

    control_left, control_mid, control_right = st.columns([1.2, 1, 1], gap="large")
    with control_left:
        visible_points = st.slider("Visible windows", min_value=10, max_value=60, value=20, step=5)
    with control_mid:
        anomalies_only = st.checkbox("Show anomaly windows only", value=False)
    with control_right:
        st.write("")
        st.write("")
        st.download_button(
            label="Download visible metrics CSV",
            data=metrics.tail(visible_points).to_csv(index=False).encode("utf-8"),
            file_name="analytics_metrics_snapshot.csv",
            mime="text/csv",
            use_container_width=True,
        )

    display_metrics = prepare_display_metrics(metrics, points=visible_points)
    dead_letter_summary = summarize_dead_letters(dead_letters)
    total_events = int(display_metrics["event_count"].sum())
    anomaly_windows = int(metrics["anomaly_detected"].sum())
    avg_revenue = display_metrics["total_revenue"].mean()
    dead_letter_count = int(len(dead_letters))
    top_reason = dead_letter_summary.iloc[0]["invalid_reason"] if not dead_letter_summary.empty else "none"

    c1, c2, c3, c4 = st.columns(4, gap="small")
    with c1:
        render_metric_card("Latest Revenue", f"${latest['total_revenue']:,.2f}", f"Change vs previous window ${latest['total_revenue'] - prev['total_revenue']:+,.2f}")
    with c2:
        render_metric_card("Latest Active Users", format_count(int(latest["active_users"])), f"Change vs previous window {int(latest['active_users'] - prev['active_users']):+d}")
    with c3:
        render_metric_card("Dead-Letter Events", format_count(dead_letter_count), f"Top reason {top_reason} | Events in view {format_count(total_events)}")
    with c4:
        render_metric_card("Pipeline Status", status["label"], f"Last window {latest['timestamp'].strftime('%H:%M')} | {status['footnote']}")

    top_left, top_right = st.columns(2)
    with top_left:
        st.markdown('<div class="section-title">Revenue per Minute</div>', unsafe_allow_html=True)
        st.altair_chart(build_revenue_chart(display_metrics, threshold), use_container_width=True)
    with top_right:
        st.markdown('<div class="section-title">Active Users per Minute</div>', unsafe_allow_html=True)
        st.altair_chart(build_users_chart(display_metrics), use_container_width=True)

    mid_left, mid_right = st.columns([1.45, 1])
    with mid_left:
        st.markdown('<div class="section-title">Event Distribution by Minute</div>', unsafe_allow_html=True)
        st.altair_chart(build_events_chart(display_metrics), use_container_width=True)
    with mid_right:
        st.markdown('<div class="section-title">Operational Notes</div>', unsafe_allow_html=True)
        st.markdown(
            f"""
            <div class="notes-card">
                <strong>Revenue spike threshold:</strong> ${threshold:,.0f}<br><br>
                <strong>Streaming-safe active users:</strong> approximate distinct count to stay compatible with Structured Streaming.<br><br>
                <strong>Duplicate handling:</strong> Spark drops duplicate <code>event_id</code> values before aggregation.<br><br>
                <strong>Data quality path:</strong> malformed events are persisted to <code>dead_letter_events</code> for review instead of silently disappearing.
            </div>
            """,
            unsafe_allow_html=True,
        )

    lower_left, lower_right = st.columns([1, 1])
    with lower_left:
        st.markdown('<div class="section-title">Aggregated Windows</div>', unsafe_allow_html=True)
        table_frame = metrics[metrics["anomaly_detected"]].copy() if anomalies_only else metrics.copy()
        st.dataframe(
            table_frame[["timestamp", "total_revenue", "event_count", "active_users", "anomaly_detected"]]
            .sort_values("timestamp", ascending=False)
            .head(20),
            use_container_width=True,
            hide_index=True,
        )
    with lower_right:
        st.markdown('<div class="section-title">Dead-Letter Summary</div>', unsafe_allow_html=True)
        if dead_letter_summary.empty:
            st.info("No invalid events captured yet.")
        else:
            st.altair_chart(build_dead_letter_chart(dead_letter_summary), use_container_width=True)
            recent_dead_letters = dead_letters[["created_at", "invalid_reason", "raw_payload"]].copy()
            recent_dead_letters["raw_payload"] = recent_dead_letters["raw_payload"].map(truncate_payload)
            st.dataframe(recent_dead_letters.head(12), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    render_dashboard()
