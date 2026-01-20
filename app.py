import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import os

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="ACB Analytics", page_icon="🏀", layout="wide")

# --- CONEXIÓN A BIGQUERY ---
@st.cache_resource
def get_db_client():
    # Intenta leer de los secretos de Streamlit (Nube)
    if "gcp_service_account" in st.secrets:
        try:
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(credentials=creds, project="acb-lineups")
        except Exception as e:
            st.error(f"❌ Error leyendo Secrets: {e}")
            return None
    
    # Si no, intenta leer archivo local (PC)
    elif os.path.exists("acb-credentials.json"):
        creds = service_account.Credentials.from_service_account_file("acb-credentials.json")
        return bigquery.Client(credentials=creds, project="acb-lineups")
    else:
        st.error("❌ No se encuentran las credenciales. Revisa los Secrets de Streamlit.")
        return None

client = get_db_client()

# --- FUNCIONES SQL (DIRECCIÓN FIJA) ---
def get_available_games():
    # Dirección escrita a fuego: proyecto.dataset.tabla
    query = """
        SELECT DISTINCT GameID, Season, Week, Team, Location 
        FROM `acb-lineups.acb_data.lineups`
        ORDER BY GameID DESC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"🚨 Error SQL al buscar partidos: {e}")
        return pd.DataFrame()

def get_lineups(game_id, team_code):
    query = f"""
        SELECT * FROM `acb-lineups.acb_data.lineups`
        WHERE GameID = {game_id} AND Team = '{team_code}'
        ORDER BY Seconds DESC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"🚨 Error SQL al buscar quintetos: {e}")
        return pd.DataFrame()

# --- INTERFAZ ---
st.sidebar.image("https://static.acb.com/img/www/logo-acb-negro.png", width=150)
st.sidebar.title("Filtros de Informe")

if client:
    # 1. Cargar lista de partidos
    df_games_index = get_available_games()
    
    if df_games_index.empty:
        st.warning("⚠️ No se han podido leer partidos. Revisa la conexión o si la BBDD está vacía.")
        st.stop()

    # 2. Selectores
    temporadas = df_games_index['Season'].unique()
    temp_sel = st.sidebar.selectbox("Temporada", temporadas)
    
    df_temp = df_games_index[df_games_index['Season'] == temp_sel]
    equipos = sorted(df_temp['Team'].unique())
    team_sel = st.sidebar.selectbox("Equipo", equipos)
    
    df_team = df_temp[df_temp['Team'] == team_sel]
    df_team['Label'] = df_team.apply(lambda x: f"Jornada {x['Week']} ({x['Location']})", axis=1)
    
    game_label = st.sidebar.selectbox("Partido", df_team['Label'].unique())
    game_id = df_team[df_team['Label'] == game_label]['GameID'].iloc[0]

    # --- PÁGINA PRINCIPAL ---
    st.title(f"🏀 Informe de Quintetos: {team_sel}")
    st.caption(f"Temporada {temp_sel} | {game_label}")

    df_lineups = get_lineups(game_id, team_sel)

    if df_lineups.empty:
        st.info("No hay datos de quintetos para este partido.")
    else:
        # --- TABLA VISUAL ---
        def make_pretty_table(df):
            def get_img(pid, name):
                if not pid: return ""
                clean_id = str(pid).replace("P","").strip()
                img_url = f"https://static.acb.com/img/jugadores/{clean_id}.jpg"
                return f"""<div style='text-align:center; width:60px;'>
                            <img src='{img_url}' style='width:45px;height:45px;border-radius:50%;object-fit:cover;border:2px solid #eee;' 
                            onerror="this.onerror=null;this.src='https://via.placeholder.com/45?text=ACB';"><br>
                            <span style='font-size:9px;font-weight:bold;'>{name}</span>
                           </div>"""

            html = "<table style='width:100%; border-collapse:collapse; font-family:sans-serif;'>"
            html += "<tr style='background:#262730; color:white; font-size:12px;'>"
            html += "<th colspan='5' style='padding:8px;'>QUINTETO EN PISTA</th>"
            html += "<th>TIEMPO</th><th>+/-</th><th>PTS</th><th>RIV</th><th>RITMO</th><th>T2%</th><th>T3%</th></tr>"
            
            for _, row in df.iterrows():
                bg_color = "#e6fffa" if row['PlusMinus'] > 0 else "#fff5f5"
                color_text = "#004d40" if row['PlusMinus'] > 0 else "#820000"
                
                html += f"<tr style='border-bottom:1px solid #ddd; background-color:{bg_color};'>"
                for i in range(1,6):
                    html += f"<td style='padding:5px;'>{get_img(row[f'ID{i}'], row[f'J{i}'])}</td>"
                
                html += f"<td style='text-align:center; font-weight:bold;'>{row['Time']}</td>"
                html += f"<td style='text-align:center; font-weight:bold; font-size:16px; color:{color_text};'>{int(row['PlusMinus'])}</td>"
                html += f"<td style='text-align:center;'>{row['PF']}</td>"
                html += f"<td style='text-align:center;'>{row['PA']}</td>"
                html += f"<td style='text-align:center;'>{row['PACE']}</td>"
                html += f"<td style='text-align:center;'>{row['T2_Pct']}%</td>"
                html += f"<td style='text-align:center;'>{row['T3_Pct']}%</td>"
                html += "</tr>"
            html += "</table>"
            return html

        st.markdown(make_pretty_table(df_lineups), unsafe_allow_html=True)
        st.download_button("📥 Descargar Informe", make_pretty_table(df_lineups), file_name="informe.html", mime="text/html")
