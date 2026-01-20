import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import os

# --- CONFIGURACIÓN (Pon tus datos aquí) ---
PROJECT_ID = 'tfm-acb-scouting'  # <--- TU ID DE PROYECTO
DATASET_ID = 'acb_data'
CREDENTIALS_FILE = 'acb-credentials.json' # El archivo que tienes en la carpeta

st.set_page_config(page_title="ACB Analytics", page_icon="🏀", layout="wide")

# --- CONEXIÓN A BIGQUERY ---
@st.cache_resource
def get_db_client():
    # Intenta leer de los secretos de Streamlit (para cuando esté en la nube)
    if "gcp_service_account" in st.secrets:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=creds, project=PROJECT_ID)
    
    # Si no, intenta leer el archivo local (para tu PC)
    elif os.path.exists(CREDENTIALS_FILE):
        creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        return bigquery.Client(credentials=creds, project=PROJECT_ID)
    else:
        st.error("❌ No se encuentran las credenciales de Google Cloud.")
        return None

client = get_db_client()

# --- FUNCIONES SQL ---
def get_available_games():
    query = f"""
        SELECT DISTINCT GameID, Season, Week, Team, Location 
        FROM `{PROJECT_ID}.{DATASET_ID}.lineups`
        ORDER BY GameID DESC
    """
    return client.query(query).to_dataframe()

def get_lineups(game_id, team_code):
    # OJO: Usamos los nombres "sanitizados" (PlusMinus, T2_Pct...)
    query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.lineups`
        WHERE GameID = {game_id} AND Team = '{team_code}'
        ORDER BY Seconds DESC
    """
    return client.query(query).to_dataframe()

# --- INTERFAZ (SIDEBAR) ---
st.sidebar.image("https://static.acb.com/img/www/logo-acb-negro.png", width=150)
st.sidebar.title("Filtros de Informe")

# 1. Cargar Partidos Disponibles
if client:
    df_games_index = get_available_games()
    
    if df_games_index.empty:
        st.warning("⚠️ La base de datos está vacía. Ejecuta el script de carga primero.")
        st.stop()

    # Selector de Temporada
    temporadas = df_games_index['Season'].unique()
    temp_sel = st.sidebar.selectbox("Temporada", temporadas)
    
    # Filtrar equipos de esa temporada
    df_temp = df_games_index[df_games_index['Season'] == temp_sel]
    equipos = sorted(df_temp['Team'].unique())
    team_sel = st.sidebar.selectbox("Equipo", equipos)
    
    # Filtrar partidos de ese equipo
    df_team = df_temp[df_temp['Team'] == team_sel]
    # Creamos una etiqueta bonita para el selector (ej: "Jornada 16 vs VBC")
    df_team['Label'] = df_team.apply(lambda x: f"Jornada {x['Week']} ({x['Location']})", axis=1)
    
    game_label = st.sidebar.selectbox("Partido", df_team['Label'].unique())
    
    # Obtener el ID del partido seleccionado
    game_id = df_team[df_team['Label'] == game_label]['GameID'].iloc[0]

    # --- PÁGINA PRINCIPAL ---
    st.title(f"🏀 Informe de Quintetos: {team_sel}")
    st.caption(f"Temporada {temp_sel} | {game_label}")

    # Cargar datos detallados
    df_lineups = get_lineups(game_id, team_sel)

    # --- RENDERIZADO VISUAL CON FOTOS ---
    def make_pretty_table(df):
        # Seleccionamos columnas clave y renombramos para que quede bonito
        cols_visual = ['ID1','ID2','ID3','ID4','ID5', 
                       'Time', 'PlusMinus', 'PF', 'PA', 'PACE', 'T2_Pct', 'T3_Pct']
        
        # HTML Helper para las fotos
        def get_img(pid, name):
            if not pid: return ""
            clean_id = str(pid).replace("P","").strip()
            # Foto: ACB suele usar esta ruta
            img_url = f"https://static.acb.com/img/jugadores/{clean_id}.jpg"
            # Fallback a logo si falla (truco HTML)
            return f"""<div style='text-align:center; width:60px;'>
                        <img src='{img_url}' style='width:45px;height:45px;border-radius:50%;object-fit:cover;border:2px solid #eee;' 
                        onerror="this.onerror=null;this.src='https://via.placeholder.com/45?text=ACB';"><br>
                        <span style='font-size:9px;font-weight:bold;'>{name}</span>
                       </div>"""

        # Construir tabla HTML a mano para control total
        html = "<table style='width:100%; border-collapse:collapse; font-family:sans-serif;'>"
        # Header
        html += "<tr style='background:#262730; color:white; font-size:12px;'>"
        html += "<th colspan='5' style='padding:8px;'>QUINTETO EN PISTA</th>"
        html += "<th>TIEMPO</th><th>+/-</th><th>PTS</th><th>RIV</th><th>RITMO</th><th>T2%</th><th>T3%</th></tr>"
        
        for _, row in df.iterrows():
            bg_color = "#e6fffa" if row['PlusMinus'] > 0 else "#fff5f5"
            color_text = "#004d40" if row['PlusMinus'] > 0 else "#820000"
            
            html += f"<tr style='border-bottom:1px solid #ddd; background-color:{bg_color};'>"
            
            # Las 5 Fotos
            for i in range(1,6):
                html += f"<td style='padding:5px;'>{get_img(row[f'ID{i}'], row[f'J{i}'])}</td>"
            
            # Datos
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
    
    # Botón de Descarga (Fake PDF por ahora, baja HTML)
    st.download_button("📥 Descargar Informe", make_pretty_table(df_lineups), file_name="informe.html", mime="text/html")