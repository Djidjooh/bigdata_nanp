import streamlit as st
import pandas as pd
import s3fs
from dotenv import load_dotenv
import os
from streamlit_autorefresh import st_autorefresh

# Charge les variables du fichier .env situé dans le même dossier
load_dotenv()

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="Bank Sandaga | Client Insights",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- STYLE CSS PERSONNALISÉ ---
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    .status-active { color: #28a745; font-weight: bold; }
    .status-inactive { color: #dc3545; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# --- AUTO-REFRESH (Toutes les 30 secondes) ---
# Ce composant force Streamlit à ré-exécuter tout le script
#count = st_autorefresh(interval=10000, limit=100, key="framerateset")


## --- CONNEXION MINIO ---
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "client-redpanda")
TOPIC_NAME = os.getenv("TOPIC_NAME", "bank_sandaga.REDPANDA.TYROK.client")
# Récupération dynamique des infos Docker
MINIO_CONF = {
    "key": os.getenv("MINIO_ACCESS_KEY", "admin"),
    "secret": os.getenv("MINIO_SECRET_KEY", "password123"),
    "client_kwargs": {
        # Dans Docker, on utilise le nom du service 'minio' au lieu de 'localhost'
        "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    }
}

# 2. Rafraîchissement auto toutes les 15s
st_autorefresh(interval=15000, key="datarefresh")

@st.cache_data(ttl=10) # seconds
def load_data():
    path = f"s3://{MINIO_BUCKET}/topics/{TOPIC_NAME}/"
    try:
        # Lecture avec pyarrow pour récupérer les partitions (year, month, day, hour)
        df = pd.read_parquet(path, storage_options=MINIO_CONF, engine='pyarrow')
        # Conversion propre des types
        cols_to_fix = ['year', 'month', 'day', 'hour']
        for col in cols_to_fix:
            df[col] = pd.to_numeric(df[col])
        return df
    except Exception as e:
        st.error(f"⚠️ Erreur de connexion au stockage : {e}")
        return pd.DataFrame()


    except Exception as e:
        # Ne s'affiche que si le bucket entier est inaccessible
        st.error(f"⚠️ Erreur critique stockage : {e}")
        return pd.DataFrame()

# --- SIDEBAR ---
with st.sidebar:
    st.image("https://flaticon.com", width=100)
    st.title("Navigation")
    
    search_query = st.text_input("🔍 Rechercher un client (Nom/Code)", "")
    
    st.divider()
    if st.button("🔄 Rafraîchir les données", use_container_width=True, type="primary"):
        st.cache_data.clear()
        st.rerun()
    
    st.caption("Flux synchronisé via Redpanda & MinIO")

# --- CORPS PRINCIPAL ---
st.title("🏦 Bank Sandaga - Flux Clients")
df = load_data()

if not df.empty:
    # FILTRE DE RECHERCHE GLOBAL
    if search_query:
        df = df[df['name'].str.contains(search_query, case=False) | df['code'].str.contains(search_query, case=False)]

    # --- SECTION MÉTRIQUES ---
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Clients", len(df))
    with m2:
        active_count = len(df[df['actif'] == True])
        st.metric("Clients Actifs", active_count, f"{active_count/len(df):.1%}")
    with m3:
        st.metric("Dernière Ingestion", f"{df['day'].max():02d}/{df['month'].max():02d}")
    with m4:
        st.metric("Heure Pointe", f"{df['hour'].mode()[0]}h")

    st.divider()

    # --- AFFICHAGE PAR ANNÉE (TABS) ---
    years = sorted(df['year'].unique(), reverse=True)
    if years:
        year_tabs = st.tabs([f"Année {y}" for y in years])

        for i, year in enumerate(years):
            with year_tabs[i]:
                months = sorted(df[df['year'] == year]['month'].unique(), reverse=True)
                
                for month in months:
                    with st.expander(f"📂 Mois : {month:02d} / {year}", expanded=(month == months[0])):
                        monthly_df = df[(df['year'] == year) & (df['month'] == month)]
                        
                        # Mise en forme du tableau
                        display_df = monthly_df[['id', 'code', 'name', 'actif', 'day', 'hour']].copy()
                        display_df = display_df.sort_values(['day', 'hour'], ascending=False)
                        
                        # Configuration de l'affichage des colonnes (Colonnes éditables ou stylisées)
                        st.dataframe(
                            display_df,
                            column_config={
                                "actif": st.column_config.CheckboxColumn("Statut Actif"),
                                "id": st.column_config.NumberColumn("ID", format="%d"),
                                "day": "Jour",
                                "hour": "Heure (h)"
                            },
                            use_container_width=True,
                            hide_index=True,
                            key=f"data_{year}_{month}"
                        )
                        st.caption(f"Fichiers Parquet analysés pour ce mois : {len(monthly_df)}")
    else:
        st.info("Aucune donnée disponible pour les critères sélectionnés.")

else:
    st.warning("📭 Le bucket 'client-redpanda' semble vide.")

# --- FOOTER ---
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: grey;'>Dashboard Temps Réel | Données Colonnaires Parquet</div>", 
    unsafe_allow_html=True
)
