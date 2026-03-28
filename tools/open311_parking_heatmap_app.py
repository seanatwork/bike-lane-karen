from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd
import pydeck as pdk
import streamlit as st


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)


def _isoformat_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@st.cache_data(show_spinner=False)
def load_bins(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql(
            """
            SELECT window_start, window_end, bin_precision, bin_lat, bin_long, bin_id,
                   count_requests, count_open, count_closed
            FROM open311_parking_heatmap_bins
            """,
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        return df

    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    return df


def main() -> None:
    st.set_page_config(page_title="Austin Parking Enforcement Heatmap (Open311)", layout="wide")
    st.title("Austin Parking Enforcement Heatmap (Open311)")
    st.caption(
        "Aggregated bins of Austin 3-1-1 Open311 service requests. "
        "This is **not** a parking citation map; it visualizes requests for enforcement."
    )

    with st.sidebar:
        st.header("Data")
        db_path = st.text_input("SQLite DB path", value="311_categories.db")
        st.caption("Run ingestion + aggregation scripts first, then point this at the same DB.")

    df = load_bins(db_path)
    if df.empty:
        st.error(
            "No aggregated bins found. Run `open311_ingest.py ingest ...` then "
            "`open311_aggregate_heatmap.py run ...` to populate `open311_parking_heatmap_bins`."
        )
        return

    with st.sidebar:
        st.header("Filters")
        precisions = sorted(df["bin_precision"].unique().tolist())
        precision = st.selectbox("Bin precision (decimal places)", precisions, index=0)

        df_p = df[df["bin_precision"] == precision].copy()
        windows = (
            df_p[["window_start", "window_end"]]
            .drop_duplicates()
            .sort_values(["window_start", "window_end"])
            .reset_index(drop=True)
        )
        window_labels = [
            f"{ws.strftime('%Y-%m-%d')} → {we.strftime('%Y-%m-%d')}"
            for ws, we in zip(windows["window_start"], windows["window_end"])
        ]
        window_idx = st.selectbox("Window", range(len(window_labels)), format_func=lambda i: window_labels[i])

        selected_ws = windows.loc[window_idx, "window_start"]
        selected_we = windows.loc[window_idx, "window_end"]
        st.caption(f"Selected: `{_isoformat_z(selected_ws.to_pydatetime())}` → `{_isoformat_z(selected_we.to_pydatetime())}`")

        min_count = st.slider("Min count (already suppressed in aggregation)", 1, 25, 3)

    df_w = df_p[(df_p["window_start"] == selected_ws) & (df_p["window_end"] == selected_we)].copy()
    df_w = df_w[df_w["count_requests"] >= min_count]

    if df_w.empty:
        st.warning("No bins match your filters.")
        return

    # Center map around Austin-ish
    center_lat = float(df_w["bin_lat"].mean())
    center_lon = float(df_w["bin_long"].mean())

    layer = pdk.Layer(
        "HeatmapLayer",
        data=df_w,
        get_position="[bin_long, bin_lat]",
        get_weight="count_requests",
        radiusPixels=60,
    )

    view_state = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=11, pitch=0)
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            "text": "Count: {count_requests}\nOpen: {count_open}\nClosed: {count_closed}",
        },
        map_style="mapbox://styles/mapbox/dark-v10",
    )

    col1, col2 = st.columns([2, 1], gap="large")
    with col1:
        st.subheader("Heatmap")
        st.pydeck_chart(deck, use_container_width=True)

    with col2:
        st.subheader("Summary")
        st.metric("Bins", int(len(df_w)))
        st.metric("Total requests (binned)", int(df_w["count_requests"].sum()))
        st.metric("Open", int(df_w["count_open"].sum()))
        st.metric("Closed", int(df_w["count_closed"].sum()))
        st.dataframe(
            df_w.sort_values("count_requests", ascending=False).head(25),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()

