# app.py
# FPL Decision-Support Dashboard — Interactive Streamlit App
# Features Position-Aware Strategy (DEF >= £5.5m, GKP >= £5.0m, MID/FWD >= £9.0m)!

import os
import sys
import yaml
import re
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Set Page Config
st.set_page_config(
    page_title="FPL Decision-Support Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Premium Custom Styling (Dark Theme & Glassmorphism)
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
        color: #ffffff;
    }
    .stMetric {
        background: rgba(255, 255, 255, 0.05);
        border-radius: 10px;
        padding: 15px;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    h1, h2, h3 {
        color: #00ff87 !important;
        font-family: 'Inter', sans-serif;
    }
    </style>
""", unsafe_allow_html=True)

# Header Banner
st.title("⚽ FPL Decision-Support Dashboard")
st.markdown("*Data-driven, 2-Tier diagnostic analytics for Set & Forget Core + Rolling Transfers.*")

# Load Config
config_path = "config/config.yaml" if os.path.exists("config/config.yaml") else "../../config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

db_gold = config["databases"]["gold"]
db_conn = config.get("databricks_connection", {})

# Sidebar Controls
st.sidebar.header("🎛️ Dashboard Controls")

with st.sidebar.expander("🔑 Databricks Cloud Connection", expanded=True):
    server_hostname = st.text_input("Server Hostname", value=db_conn.get("server_hostname", "dbc-4db848ba-6cbe.cloud.databricks.com"))
    http_path = st.text_input("HTTP Path", value=db_conn.get("http_path", ""))
    access_token = st.text_input("Personal Access Token", value=db_conn.get("access_token", ""), type="password")
    connect_btn = st.button("Connect to Databricks 🔌", use_container_width=True)

if st.sidebar.button("Clear Cache & Refresh Live Data 🔄", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

# Default to ALL positions (GKP, DEF, MID, FWD) so every position is suggested!
pos_filter = st.sidebar.multiselect("Filter Position", ["GKP", "DEF", "MID", "FWD"], default=["GKP", "DEF", "MID", "FWD"])
max_budget = st.sidebar.slider("Max Budget (£m)", min_value=4.0, max_value=15.0, value=15.0, step=0.5)

# Built-in High-Fidelity Dataset Generators Across ALL Positions
def get_sample_data(table_name):
    if table_name == "captaincy_fit":
        return pd.DataFrame([
            {"captaincy_rank": 1, "web_name": "Haaland", "team_name": "Man City", "position_name": "FWD", "price_gbp": 15.5, "haul_frequency_percent": 42.5, "captaincy_fit_score": 3.15},
            {"captaincy_rank": 2, "web_name": "Palmer", "team_name": "Chelsea", "position_name": "MID", "price_gbp": 9.5, "haul_frequency_percent": 35.0, "captaincy_fit_score": 2.85},
            {"captaincy_rank": 3, "web_name": "Saka", "team_name": "Arsenal", "position_name": "MID", "price_gbp": 10.0, "haul_frequency_percent": 32.0, "captaincy_fit_score": 2.75},
            {"captaincy_rank": 1, "web_name": "Gabriel", "team_name": "Arsenal", "position_name": "DEF", "price_gbp": 8.0, "haul_frequency_percent": 22.0, "captaincy_fit_score": 2.30},
            {"captaincy_rank": 2, "web_name": "Alexander-Arnold", "team_name": "Liverpool", "position_name": "DEF", "price_gbp": 7.0, "haul_frequency_percent": 28.5, "captaincy_fit_score": 2.25},
            {"captaincy_rank": 1, "web_name": "Raya", "team_name": "Arsenal", "position_name": "GKP", "price_gbp": 5.5, "haul_frequency_percent": 18.0, "captaincy_fit_score": 2.10},
        ])
    elif table_name == "fixture_planner":
        return pd.DataFrame([
            {"team_name": "Arsenal", "short_name": "ARS", "avg_5gw_fdr": 2.2, "next_gw_1": "WOL (H) FDR:2", "next_gw_2": "LEI (A) FDR:2", "next_gw_3": "SOU (H) FDR:2", "next_gw_4": "EVE (A) FDR:3", "next_gw_5": "BOU (H) FDR:2"},
            {"team_name": "Liverpool", "short_name": "LIV", "avg_5gw_fdr": 2.6, "next_gw_1": "IPS (A) FDR:2", "next_gw_2": "BRE (H) FDR:2", "next_gw_3": "WHU (A) FDR:3", "next_gw_4": "NFO (H) FDR:2", "next_gw_5": "CHE (A) FDR:4"},
            {"team_name": "Man City", "short_name": "MCI", "avg_5gw_fdr": 2.8, "next_gw_1": "CHE (A) FDR:4", "next_gw_2": "IPS (H) FDR:2", "next_gw_3": "WHU (A) FDR:3", "next_gw_4": "BRE (H) FDR:2", "next_gw_5": "ARS (H) FDR:3"},
            {"team_name": "Aston Villa", "short_name": "AVL", "avg_5gw_fdr": 3.0, "next_gw_1": "WHU (A) FDR:3", "next_gw_2": "ARS (H) FDR:4", "next_gw_3": "LEI (A) FDR:2", "next_gw_4": "EVE (H) FDR:2", "next_gw_5": "WOL (A) FDR:3"},
            {"team_name": "Ipswich Town", "short_name": "IPS", "avg_5gw_fdr": 4.2, "next_gw_1": "LIV (H) FDR:5", "next_gw_2": "MCI (A) FDR:5", "next_gw_3": "ARS (A) FDR:5", "next_gw_4": "FUL (H) FDR:3", "next_gw_5": "BHA (A) FDR:3"},
        ])
    elif table_name == "value_scores":
        return pd.DataFrame([
            {"web_name": "Haaland", "team_name": "Man City", "position_name": "FWD", "price_gbp": 15.5, "ownership_percent": 73.5, "form": 8.5, "avg_upcoming_fdr": 2.8, "value_score": 3.15, "strategy_tier": "🛡️ Season Anchor (Set & Forget)"},
            {"web_name": "Palmer", "team_name": "Chelsea", "position_name": "MID", "price_gbp": 9.5, "ownership_percent": 55.9, "form": 7.2, "avg_upcoming_fdr": 2.3, "value_score": 2.85, "strategy_tier": "🛡️ Season Anchor (Set & Forget)"},
            {"web_name": "Saka", "team_name": "Arsenal", "position_name": "MID", "price_gbp": 10.0, "ownership_percent": 34.2, "form": 7.5, "avg_upcoming_fdr": 2.2, "value_score": 2.75, "strategy_tier": "🛡️ Season Anchor (Set & Forget)"},
            {"web_name": "Gabriel", "team_name": "Arsenal", "position_name": "DEF", "price_gbp": 8.0, "ownership_percent": 27.1, "form": 6.8, "avg_upcoming_fdr": 2.2, "value_score": 2.65, "strategy_tier": "🛡️ Season Anchor (Set & Forget)"},
            {"web_name": "Alexander-Arnold", "team_name": "Liverpool", "position_name": "DEF", "price_gbp": 7.0, "ownership_percent": 29.5, "form": 6.5, "avg_upcoming_fdr": 2.6, "value_score": 2.55, "strategy_tier": "🛡️ Season Anchor (Set & Forget)"},
            {"web_name": "Saliba", "team_name": "Arsenal", "position_name": "DEF", "price_gbp": 6.0, "ownership_percent": 32.1, "form": 6.2, "avg_upcoming_fdr": 2.2, "value_score": 2.50, "strategy_tier": "🛡️ Season Anchor (Set & Forget)"},
            {"web_name": "Raya", "team_name": "Arsenal", "position_name": "GKP", "price_gbp": 5.5, "ownership_percent": 24.1, "form": 6.0, "avg_upcoming_fdr": 2.2, "value_score": 2.45, "strategy_tier": "🛡️ Season Anchor (Set & Forget)"},
            {"web_name": "Rogers", "team_name": "Aston Villa", "position_name": "MID", "price_gbp": 7.5, "ownership_percent": 22.1, "form": 5.8, "avg_upcoming_fdr": 3.0, "value_score": 2.40, "strategy_tier": "🔄 Rolling Transfer Target"},
            {"web_name": "Consa", "team_name": "Aston Villa", "position_name": "DEF", "price_gbp": 4.5, "ownership_percent": 12.5, "form": 5.2, "avg_upcoming_fdr": 3.0, "value_score": 2.20, "strategy_tier": "🔄 Rolling Transfer Target"},
            {"web_name": "Mbeumo", "team_name": "Brentford", "position_name": "MID", "price_gbp": 7.0, "ownership_percent": 18.5, "form": 6.1, "avg_upcoming_fdr": 2.4, "value_score": 2.15, "strategy_tier": "🔄 Rolling Transfer Target"},
            {"web_name": "Pickford", "team_name": "Everton", "position_name": "GKP", "price_gbp": 5.0, "ownership_percent": 15.2, "form": 5.0, "avg_upcoming_fdr": 2.8, "value_score": 2.10, "strategy_tier": "🔄 Rolling Transfer Target"},
        ])
    elif table_name == "differentials":
        return pd.DataFrame([
            {"differential_rank": 1, "web_name": "Ekitiké", "team_name": "Liverpool", "position_name": "FWD", "price_gbp": 7.5, "ownership_percent": 0.2, "gi_per_90": 0.85, "total_xg": 12.4, "total_xa": 4.2},
            {"differential_rank": 2, "web_name": "Wood", "team_name": "Nott'm Forest", "position_name": "FWD", "price_gbp": 6.0, "ownership_percent": 1.7, "gi_per_90": 0.72, "total_xg": 11.2, "total_xa": 2.1},
            {"differential_rank": 3, "web_name": "Rogers", "team_name": "Aston Villa", "position_name": "MID", "price_gbp": 7.5, "ownership_percent": 8.5, "gi_per_90": 0.68, "total_xg": 8.5, "total_xa": 5.4},
            {"differential_rank": 4, "web_name": "Consa", "team_name": "Aston Villa", "position_name": "DEF", "price_gbp": 4.5, "ownership_percent": 4.2, "gi_per_90": 0.35, "total_xg": 2.4, "total_xa": 1.8},
        ])
    elif table_name == "price_momentum":
        return pd.DataFrame([
            {"web_name": "Haaland", "team_name": "Man City", "position_name": "FWD", "price_gbp": 15.5, "net_transfers_event": 45200, "momentum_direction": "Price Rise Candidate 📈"},
            {"web_name": "Gabriel", "team_name": "Arsenal", "position_name": "DEF", "price_gbp": 8.0, "net_transfers_event": 28400, "momentum_direction": "Price Rise Candidate 📈"},
            {"web_name": "Watkins", "team_name": "Aston Villa", "position_name": "FWD", "price_gbp": 9.0, "net_transfers_event": -18400, "momentum_direction": "Price Fall Candidate 📉"},
        ])
    elif table_name == "underlying_stats":
        return pd.DataFrame([
            {"web_name": "Jackson", "team_name": "Chelsea", "position_name": "FWD", "price_gbp": 7.5, "total_points": 142, "total_goals": 14, "total_xg": 18.2, "total_xa": 5.1, "xg_delta": 4.2, "due_a_return_flag": "Due a Goal 🎯 (Underperforming xG)"},
            {"web_name": "Gabriel", "team_name": "Arsenal", "position_name": "DEF", "price_gbp": 8.0, "total_points": 152, "total_goals": 5, "total_xg": 7.8, "total_xa": 2.1, "xg_delta": 2.8, "due_a_return_flag": "Due a Goal 🎯 (Underperforming xG)"},
        ])
    return pd.DataFrame()

# Data Loader Function (Local File -> Databricks Cloud -> Sample Fallback)
@st.cache_data(ttl=300)
def load_gold_table(table_name, _hostname, _http_path, _token):
    # Strategy 1: Databricks Cloud Direct Connection (if credentials entered)
    if _hostname and _http_path and _token:
        try:
            from databricks import sql
            with sql.connect(server_hostname=_hostname, http_path=_http_path, access_token=_token) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM {db_gold}.{table_name}")
                    result = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(result, columns=columns)
        except Exception as e:
            st.sidebar.error(f"Databricks Connection Error: {e}")

    # Strategy 2: Built-in Data (Instant zero-setup launch!)
    return get_sample_data(table_name)

# Feedback indicator
if http_path and access_token:
    st.sidebar.success("🟢 Databricks Credentials Configured!")

# Main Navigation Tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🛡️ Set & Forget Core", 
    "🔄 Rolling Transfers", 
    "🗓️ Fixture Heatmap", 
    "👑 Captaincy Fit", 
    "💎 Differentials", 
    "📈 Price & xG Watchlist"
])

# Helper function to filter Anchors by position price thresholds
def is_anchor(row):
    if "strategy_tier" in row and pd.notna(row["strategy_tier"]):
        return "Anchor" in str(row["strategy_tier"])
    pos = row.get("position_name", "")
    price = row.get("price_gbp", 0.0)
    own = row.get("ownership_percent", 0.0)
    if pos == "DEF":
        return price >= 5.5 or own >= 15.0
    elif pos == "GKP":
        return price >= 5.0 or own >= 15.0
    else:
        return price >= 9.0 or own >= 25.0

# -------------------------------------------------------------
# TAB 1: Set & Forget Season Core Anchors
# -------------------------------------------------------------
with tab1:
    st.subheader("🛡️ Season-Long Core Anchors Across All Positions")
    st.markdown("*Non-negotiable season holds (GKP, DEF, MID, FWD) with high point potential. Do NOT waste weekly free transfers selling these players for 1 hard fixture.*")
    df_val = load_gold_table("value_scores", server_hostname, http_path, access_token)
    
    if not df_val.empty:
        anchors = df_val[df_val.apply(is_anchor, axis=1)]
        anchors = anchors[anchors["position_name"].isin(pos_filter)] if "position_name" in anchors.columns else anchors
        anchors = anchors.sort_values(by="value_score", ascending=False)
        
        fig_anchors = px.bar(
            anchors.head(12),
            x="web_name",
            y="value_score",
            color="position_name",
            hover_data=["team_name", "price_gbp", "ownership_percent"],
            title="Top Season Core Anchors Across All Positions (GKP, DEF, MID, FWD)",
            template="plotly_dark"
        )
        st.plotly_chart(fig_anchors, use_container_width=True)
        
        st.dataframe(
            anchors[["web_name", "team_name", "position_name", "price_gbp", "ownership_percent", "value_score"]],
            use_container_width=True
        )

# -------------------------------------------------------------
# TAB 2: Rolling Transfer Candidates
# -------------------------------------------------------------
with tab2:
    st.subheader("🔄 Rolling Transfer Targets Across All Positions")
    st.markdown("*Budget & mid-price spot players (£4.5m–£8.5m) to swap in and out based on 5-gameweek fixture swings.*")
    df_val = load_gold_table("value_scores", server_hostname, http_path, access_token)
    
    if not df_val.empty:
        targets = df_val[~df_val.apply(is_anchor, axis=1)]
        targets = targets[(targets["position_name"].isin(pos_filter)) & (targets["price_gbp"] <= max_budget)] if "position_name" in targets.columns else targets
        targets = targets.sort_values(by="value_score", ascending=False)
        
        fig_targets = px.bar(
            targets.head(12),
            x="web_name",
            y="value_score",
            color="position_name",
            hover_data=["team_name", "price_gbp", "avg_upcoming_fdr"],
            title="Top 5-Gameweek Rolling Transfer Targets Across All Positions",
            template="plotly_dark"
        )
        st.plotly_chart(fig_targets, use_container_width=True)
        
        st.dataframe(
            targets[["web_name", "team_name", "position_name", "price_gbp", "ownership_percent", "avg_upcoming_fdr", "value_score"]],
            use_container_width=True
        )

# -------------------------------------------------------------
# TAB 3: Fixture Difficulty Heatmap
# -------------------------------------------------------------
with tab3:
    st.subheader("🗓️ 5-Gameweek Fixture Difficulty Heatmap Matrix")
    df_fix = load_gold_table("fixture_planner", server_hostname, http_path, access_token)
    
    if not df_fix.empty:
        # Parse numeric FDR from next_gw_1..5 string columns if needed
        for i in range(1, 6):
            col_name = f"next_gw_{i}"
            gw_label = f"GW{i}"
            if col_name in df_fix.columns:
                df_fix[gw_label] = df_fix[col_name].astype(str).str.extract(r'FDR:(\d+)')[0].astype(float).fillna(3)
        
        if "GW1" in df_fix.columns:
            heatmap_df = df_fix.set_index("team_name")[["GW1", "GW2", "GW3", "GW4", "GW5"]]
            
            fig_heat = px.imshow(
                heatmap_df,
                labels=dict(x="Gameweek", y="Team", color="FDR Rating"),
                x=["GW1", "GW2", "GW3", "GW4", "GW5"],
                y=heatmap_df.index,
                color_continuous_scale=[[0.0, "#00ff87"], [0.5, "#888888"], [1.0, "#ff0055"]],
                aspect="auto",
                title="Premier League 5-Gameweek Fixture Difficulty Heatmap"
            )
            fig_heat.update_layout(template="plotly_dark", height=500)
            st.plotly_chart(fig_heat, use_container_width=True)

        st.dataframe(
            df_fix.sort_values(by="avg_5gw_fdr", ascending=True),
            use_container_width=True
        )

# -------------------------------------------------------------
# TAB 4: Captaincy Fit
# -------------------------------------------------------------
with tab4:
    st.subheader("👑 Weekly Captaincy Recommendation Panel")
    df_cap = load_gold_table("captaincy_fit", server_hostname, http_path, access_token)
    
    if not df_cap.empty:
        filtered_cap = df_cap[df_cap["position_name"].isin(pos_filter)] if "position_name" in df_cap.columns else df_cap
        
        # Display Top 3 Picks in Metric Cards
        cols = st.columns(3)
        top_picks = filtered_cap.sort_values(by="captaincy_fit_score", ascending=False).head(3)
        
        for idx, (_, row) in enumerate(top_picks.iterrows()):
            with cols[idx]:
                st.metric(
                    label=f"Rank #{idx+1} Captain Pick",
                    value=f"{row['web_name']} ({row['team_name']})",
                    delta=f"Cap Score: {row['captaincy_fit_score']} | Haul Freq: {row['haul_frequency_percent']}%"
                )
        
        st.write("---")
        st.dataframe(
            filtered_cap[["captaincy_rank", "web_name", "team_name", "position_name", "price_gbp", "haul_frequency_percent", "captaincy_fit_score"]],
            use_container_width=True
        )

# -------------------------------------------------------------
# TAB 5: Differentials
# -------------------------------------------------------------
with tab5:
    st.subheader("💎 Low-Ownership (<10%) Differential Gem Finder")
    df_diff = load_gold_table("differentials", server_hostname, http_path, access_token)
    
    if not df_diff.empty:
        fig_diff = px.scatter(
            df_diff,
            x="ownership_percent",
            y="gi_per_90",
            size="price_gbp",
            color="position_name",
            hover_name="web_name",
            title="Differential Gems: Low Ownership % vs. High Goal Involvements per 90",
            template="plotly_dark"
        )
        st.plotly_chart(fig_diff, use_container_width=True)
        
        st.dataframe(
            df_diff[["differential_rank", "web_name", "team_name", "position_name", "price_gbp", "ownership_percent", "gi_per_90", "total_xg", "total_xa"]],
            use_container_width=True
        )

# -------------------------------------------------------------
# TAB 6: Price & xG Watchlist
# -------------------------------------------------------------
with tab6:
    st.subheader("🎯 Expected Goals (xG) & 📈 Price Transfer Activity")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### 🎯 Players 'Due a Goal' (xG > Goals)")
        df_xg = load_gold_table("underlying_stats", server_hostname, http_path, access_token)
        if not df_xg.empty:
            st.dataframe(df_xg[["web_name", "team_name", "position_name", "price_gbp", "total_goals", "total_xg", "xg_delta"]], use_container_width=True)
            
    with col2:
        st.markdown("##### 📈 Price Rise & Fall Candidates")
        df_mom = load_gold_table("price_momentum", server_hostname, http_path, access_token)
        if not df_mom.empty:
            st.dataframe(df_mom[["web_name", "team_name", "position_name", "price_gbp", "net_transfers_event", "momentum_direction"]], use_container_width=True)

# Footer
st.markdown("---")
st.caption("FPL Decision-Support System | Built with Databricks Medallion Architecture, Plotly & Streamlit.")
