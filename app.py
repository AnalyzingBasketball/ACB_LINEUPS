import streamlit as st
import pandas as pd
import requests
import re
import time
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq

# ==============================================================================
# 1. CONFIGURACIÓN
# ==============================================================================
st.set_page_config(page_title="ACB Smart Scout", page_icon="🏀", layout="wide")

# --- VARIABLES FIJAS (Temporada Actual) ---
CURRENT_SEASON = "2025"  # ID 2025 para la temporada 25/26
PROJECT_ID = "acb-lineups"
DATASET_ID = "acb_data"
API_KEY = '0dd94928-6f57-4c08-a3bd-b1b2f092976e'

HEADERS_API = {
    'x-apikey': API_KEY, 'User-Agent': 'Mozilla/5.0', 'Referer': 'https://live.acb.com/'
}
HEADERS_WEB = {'User-Agent': 'Mozilla/5.0'}

# --- CONEXIÓN CLOUD ---
@st.cache_resource
def get_client():
    if "gcp_service_account" in st.secrets:
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=creds, project=PROJECT_ID), creds
    return None, None

client, credentials = get_client()

# ==============================================================================
# 2. INTELIGENCIA (DETECTAR QUÉ FALTA)
# ==============================================================================

def get_existing_games_in_bq():
    """Pregunta a la Nube qué partidos ya tenemos guardados."""
    try:
        q = f"SELECT DISTINCT GameID FROM `{PROJECT_ID}.{DATASET_ID}.lineups` WHERE Season = '{CURRENT_SEASON}'"
        df = client.query(q).to_dataframe()
        return set(df['GameID'].tolist())
    except:
        return set() # Si falla o tabla no existe, devuelve vacío (bajará todo)

def get_played_games_on_web(jornadas_a_escanear=40):
    """Escanea ACB.com para ver qué partidos existen realmente."""
    found_games = []
    # Barra de progreso visual
    status_text = st.empty()
    prog_bar = st.progress(0)
    
    # Escaneamos Liga (1) y Copa (2)
    comps = [('1', 'Liga'), ('2', 'Copa')]
    
    total_steps = len(comps) * jornadas_a_escanear
    step = 0
    
    for comp_id, comp_name in comps:
        for j in range(1, jornadas_a_escanear + 1):
            # Actualizamos barra
            step += 1
            if step % 5 == 0: # Para no saturar visualmente
                prog_bar.progress(step / total_steps)
                status_text.text(f"📡 Escaneando ACB: {comp_name} - Jornada {j}...")

            url = f"https://www.acb.com/resultados-clasificacion/ver/temporada_id/{CURRENT_SEASON}/competicion_id/{comp_id}/jornada_numero/{j}"
            try:
                r = requests.get(url, headers=HEADERS_WEB, timeout=2)
                # Buscamos IDs de partidos en el HTML
                ids = re.findall(r'/partido/estadisticas/id/(\d+)', r.text)
                for mid in set(ids):
                    found_games.append({
                        'id': int(mid), 
                        'Week': str(j), 
                        'Comp': comp_name
                    })
            except: pass
            
    status_text.empty()
    prog_bar.empty()
    return pd.DataFrame(found_games)

# ==============================================================================
# 3. MOTOR DE PROCESAMIENTO (Scraping + Quintetos)
# ==============================================================================

def get_pbp_and_process(games_list):
    """Descarga y procesa solo los partidos de la lista."""
    all_rows = []
    
    my_bar = st.progress(0)
    
    for idx, game in enumerate(games_list):
        gid = game['id']
        # 1. Bajar PBP
        url = "https://api2.acb.com/api/matchdata/PlayByPlay/play-by-play"
        try:
            r = requests.get(url, params={'matchId': gid}, headers=HEADERS_API, timeout=5)
            if r.status_code == 200:
                data = r.json()
                raw_events = []
                if isinstance(data, list): raw_events = data
                elif isinstance(data, dict):
                    for k,v in data.items(): 
                        if isinstance(v, list): raw_events.extend(v)
                
                # Ordenar eventos
                def sort_key(e):
                    t = e.get('cronometer', "00:00")
                    if ':' in t: m,s = map(int, t.split(':')); sec = m*60+s
                    else: sec = 0
                    return (e.get('period', 0), -sec)
                raw_events.sort(key=sort_key)
                
                # Procesar Quintetos
                home_on, away_on = set(), set()
                
                for ev in raw_events:
                    if not isinstance(ev, dict): continue
                    
                    act_id = str(ev.get('idAction', 'UNK'))
                    t_str = ev.get('cronometer', "00:00")
                    is_loc = ev.get('local')
                    loc = "HOME" if is_loc is True else "AWAY"
                    
                    # Puntos
                    s_h = ev.get('homeScore', 0)
                    s_a = ev.get('awayScore', 0)
                    
                    pname = ev.get('player', {}).get('nickName')
                    pid = str(ev.get('player', {}).get('id', '')).replace("P","").strip()
                    
                    # Gestión Cambios
                    if pname:
                        target = home_on if loc == "HOME" else away_on
                        if act_id in ['599', '112']: target.add((pname, pid))
                        elif act_id == '115': 
                            # Eliminar buscando por nombre (seguro)
                            target = {x for x in target if x[0] != pname}
                            if loc == "HOME": home_on = target
                            else: away_on = target
                        else: target.add((pname, pid))
                    
                    # Snapshot del momento
                    h_list = sorted(list(home_on))[:5]
                    a_list = sorted(list(away_on))[:5]
                    
                    # Formato tuple para guardar
                    h_names = tuple([x[0] for x in h_list])
                    a_names = tuple([x[0] for x in a_list])
                    h_ids = tuple([x[1] for x in h_list])
                    a_ids = tuple([x[1] for x in a_list])
                    
                    # Seconds
                    m, s = 0, 0
                    if ':' in t_str: m,s = map(int, t_str.split(':'))
                    secs = m*60 + s
                    
                    all_rows.append({
                        'GameID': gid, 'Season': CURRENT_SEASON, 'Week': game['Week'],
                        'Period': ev.get('period'), 'Seconds': secs, 'Time': t_str,
                        'Score_Home': s_h, 'Score_Away': s_a,
                        'H_Lineup': h_names, 'A_Lineup': a_names,
                        'H_IDs': h_ids, 'A_IDs': a_ids
                    })
        except: pass
        
        my_bar.progress((idx + 1) / len(games_list))
    
    my_bar.empty()
    return pd.DataFrame(all_rows)

def calculate_stats(df):
    if df.empty: return pd.DataFrame()
    
    # Rellenar huecos (Forward Fill)
    df['H_Lineup'] = df['H_Lineup'].apply(lambda x: x if x and len(x) == 5 else None).ffill()
    df['A_Lineup'] = df['A_Lineup'].apply(lambda x: x if x and len(x) == 5 else None).ffill()
    df['H_IDs'] = df['H_IDs'].apply(lambda x: x if x and len(x) == 5 else None).ffill()
    df['A_IDs'] = df['A_IDs'].apply(lambda x: x if x and len(x) == 5 else None).ffill()
    
    stats = {}
    
    for gid, df_g in df.groupby('GameID'):
        df_g = df_g.sort_index()
        meta = df_g.iloc[0]
        prev_h, prev_a = 0, 0
        
        for i in range(len(df_g)):
            row = df_g.iloc[i]
            dur = 0
            if i < len(df_g)-1:
                if row['Period'] == df_g.iloc[i+1]['Period']:
                    dur = max(0, row['Seconds'] - df_g.iloc[i+1]['Seconds'])
            
            # HOME STATS
            if row['H_Lineup']:
                k = (gid, "HOME", row['H_Lineup'], row['H_IDs'])
                if k not in stats: stats[k] = {'Sec':0, 'PF':0, 'PA':0}
                stats[k]['Sec'] += dur
                
            # AWAY STATS
            if row['A_Lineup']:
                k = (gid, "AWAY", row['A_Lineup'], row['A_IDs'])
                if k not in stats: stats[k] = {'Sec':0, 'PF':0, 'PA':0}
                stats[k]['Sec'] += dur
            
            # PUNTOS
            diff_h = row['Score_Home'] - prev_h
            diff_a = row['Score_Away'] - prev_a
            
            if diff_h > 0:
                if row['H_Lineup']: stats[(gid, "HOME", row['H_Lineup'], row['H_IDs'])]['PF'] += diff_h
                if row['A_Lineup']: stats[(gid, "AWAY", row['A_Lineup'], row['A_IDs'])]['PA'] += diff_h
            if diff_a > 0:
                if row['A_Lineup']: stats[(gid, "AWAY", row['A_Lineup'], row['A_IDs'])]['PF'] += diff_a
                if row['H_Lineup']: stats[(gid, "HOME", row['H_Lineup'], row['H_IDs'])]['PA'] += diff_a
                
            prev_h = row['Score_Home']; prev_a = row['Score_Away']
            
    final = []
    for (gid, loc, names, ids), val in stats.items():
        if val['Sec'] > 0:
            d = {
                'GameID': gid, 'Season': CURRENT_SEASON, 'Team': 'UNK', 'Location': loc,
                'Week': df[df['GameID']==gid].iloc[0]['Week'],
                'Time': f"{int(val['Sec']//60):02d}:{int(val['Sec']%60):02d}",
                'Seconds': val['Sec'], 'PF': val['PF'], 'PA': val['PA'], 
                'PlusMinus': val['PF'] - val['PA']
            }
            for idx, (name, pid) in enumerate(zip(names, ids)):
                d[f"J{idx+1}"] = name
                d[f"ID{idx+1}"] = pid
            final.append(d)
            
    return pd.DataFrame(final)

# ==============================================================================
# 4. INTERFAZ (BOTÓN DE SINCRONIZACIÓN + VISOR)
# ==============================================================================

st.title(f"🏀 Super Scout ACB {CURRENT_SEASON}")

# --- BOTÓN DE SINCRONIZACIÓN ---
with st.expander("🔄 SINCRONIZAR DATOS (Incremental)", expanded=True):
    col_btn, col_info = st.columns([1, 3])
    
    with col_btn:
        run_sync = st.button("BUSCAR Y BAJAR PARTIDOS NUEVOS", type="primary")
        
    if run_sync:
        with st.spinner("Conectando con el cerebro..."):
            # 1. ¿Qué tenemos?
            existing_ids = get_existing_games_in_bq()
            st.write(f"💾 En Base de Datos: {len(existing_ids)} partidos.")
            
            # 2. ¿Qué hay en la web?
            df_web = get_played_games_on_web() # Escanea jornadas
            if df_web.empty:
                st.error("No se encontraron partidos en ACB.com")
            else:
                web_ids = set(df_web['id'].tolist())
                st.write(f"🌐 En ACB.com: {len(web_ids)} partidos jugados.")
                
                # 3. ¿Qué falta?
                missing_ids = web_ids - existing_ids
                
                if not missing_ids:
                    st.success("✅ ¡TODO AL DÍA! No hay partidos nuevos.")
                else:
                    st.warning(f"⚡ Se han detectado {len(missing_ids)} partidos NUEVOS. Descargando...")
                    
                    # Filtramos el dataframe web para quedarnos con los que faltan
                    games_to_process = df_web[df_web['id'].isin(missing_ids)].to_dict('records')
                    
                    # 4. Procesar
                    df_raw = get_pbp_and_process(games_to_process)
                    if not df_raw.empty:
                        df_final = calculate_stats(df_raw)
                        
                        # 5. Subir
                        if not df_final.empty:
                            table_id = f"{DATASET_ID}.lineups"
                            pandas_gbq.to_gbq(
                                df_final, table_id, project_id=PROJECT_ID, 
                                if_exists='append', credentials=credentials
                            )
                            st.balloons()
                            st.success(f"🚀 ¡ÉXITO! Se han añadido {len(missing_ids)} partidos a la base de datos.")
                            time.sleep(2)
                            st.rerun()

st.divider()

# --- VISOR (LO DE SIEMPRE) ---
q_main = f"SELECT DISTINCT GameID, Week, Location FROM `{PROJECT_ID}.{DATASET_ID}.lineups` WHERE Season = '{CURRENT_SEASON}' ORDER BY GameID DESC"
try:
    df_index = client.query(q_main).to_dataframe()
except: df_index = pd.DataFrame()

if df_index.empty:
    st.info("👆 Dale al botón de arriba para hacer la PRIMERA CARGA de datos.")
else:
    # Selectores
    col1, col2 = st.columns(2)
    with col1:
        # Selector Jornada
        jornadas = sorted(df_index['Week'].unique(), key=lambda x: int(x))
        sel_week = st.selectbox("Jornada", jornadas, index=len(jornadas)-1)
    
    with col2:
        # Selector Partido
        df_week = df_index[df_index['Week'] == sel_week]
        df_week['Label'] = df_week.apply(lambda x: f"Partido ID {x['GameID']} ({x['Location']})", axis=1)
        sel_game_lbl = st.selectbox("Partido", df_week['Label'].unique())
        
    gid = df_week[df_week['Label'] == sel_game_lbl]['GameID'].iloc[0]
    
    # Tabla
    q_data = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.lineups` WHERE GameID = {gid} ORDER BY Seconds DESC"
    df_data = client.query(q_data).to_dataframe()
    
    def make_pretty_table(df):
        def get_img(pid, name):
            if not pid or pid == "": return f"<div style='font-size:10px;'>{name[:3]}</div>"
            clean_id = str(pid).replace(".0","").strip()
            return f"""<div style='text-align:center;'><img src='https://static.acb.com/img/jugadores/JPG/{clean_id}.jpg' style='width:40px;height:40px;border-radius:50%;object-fit:cover;' onerror="this.onerror=null;this.src='https://via.placeholder.com/40';"><br><span style='font-size:9px;'>{name}</span></div>"""

        html = "<table style='width:100%; font-size:12px; border-collapse:collapse;'>"
        html += "<tr style='background:#333; color:white;'><th>QUINTETO</th><th>TIEMPO</th><th>+/-</th><th>PTS</th><th>RIV</th></tr>"
        for _, r in df.iterrows():
            bg = "#eaffea" if r['PlusMinus'] > 0 else "#ffeaea"
            color = "green" if r['PlusMinus'] > 0 else "red"
            
            # Fotos
            players_html = "<div style='display:flex; gap:5px;'>"
            for i in range(1,6): players_html += get_img(r[f'ID{i}'], r[f'J{i}'])
            players_html += "</div>"
            
            html += f"<tr style='background:{bg}; border-bottom:1px solid #ccc;'>"
            html += f"<td style='padding:5px;'>{players_html}</td>"
            html += f"<td style='text-align:center;'>{r['Time']}</td>"
            html += f"<td style='text-align:center; font-weight:bold; font-size:14px; color:{color};'>{int(r['PlusMinus'])}</td>"
            html += f"<td style='text-align:center;'>{r['PF']}</td>"
            html += f"<td style='text-align:center;'>{r['PA']}</td></tr>"
        return html + "</table>"

    st.write(f"### Quintetos del Partido {gid}")
    st.markdown(make_pretty_table(df_data), unsafe_allow_html=True)
