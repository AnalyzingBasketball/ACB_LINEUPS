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

# --- VARIABLES FIJAS ---
CURRENT_SEASON = "2025" 
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
# 2. FUNCIONES DE LÓGICA
# ==============================================================================

def get_existing_games_in_bq():
    """Pregunta a BigQuery qué tenemos ya."""
    try:
        q = f"SELECT DISTINCT GameID FROM `{PROJECT_ID}.{DATASET_ID}.lineups` WHERE Season = '{CURRENT_SEASON}'"
        df = client.query(q).to_dataframe()
        return set(df['GameID'].tolist())
    except:
        return set()

def get_played_games_on_web():
    """Escanea ACB.com rápido."""
    found_games = []
    # Escaneamos hasta la jornada 20 (para ir rápido, puedes subirlo a 40)
    # Liga (1) y Copa (2)
    urls_to_scan = []
    for j in range(1, 35):
        urls_to_scan.append((f"https://www.acb.com/resultados-clasificacion/ver/temporada_id/{CURRENT_SEASON}/competicion_id/1/jornada_numero/{j}", str(j)))
        # urls_to_scan.append((f"https://www.acb.com/resultados-clasificacion/ver/temporada_id/{CURRENT_SEASON}/competicion_id/2/jornada_numero/{j}", str(j)))
    
    status = st.empty()
    bar = st.progress(0)
    
    for i, (url, week) in enumerate(urls_to_scan):
        if i % 5 == 0: 
            status.text(f"📡 Escaneando Jornada {week}...")
            bar.progress((i+1)/len(urls_to_scan))
        try:
            r = requests.get(url, headers=HEADERS_WEB, timeout=2)
            ids = re.findall(r'/partido/estadisticas/id/(\d+)', r.text)
            for mid in set(ids):
                found_games.append({'id': int(mid), 'Week': week})
        except: pass
    
    status.empty(); bar.empty()
    return pd.DataFrame(found_games)

def process_single_game(gid, season, week):
    """Baja y procesa UN solo partido."""
    url = "https://api2.acb.com/api/matchdata/PlayByPlay/play-by-play"
    try:
        r = requests.get(url, params={'matchId': gid}, headers=HEADERS_API, timeout=5)
        if r.status_code != 200: return []
        
        data = r.json()
        raw_events = []
        if isinstance(data, list): raw_events = data
        elif isinstance(data, dict):
            for k,v in data.items(): 
                if isinstance(v, list): raw_events.extend(v)
        
        # Ordenar cronológicamente
        def sort_key(e):
            t = e.get('cronometer', "00:00")
            if ':' in t: m,s = map(int, t.split(':')); sec = m*60+s
            else: sec = 0
            return (e.get('period', 0), -sec)
        raw_events.sort(key=sort_key)
        
        processed_rows = []
        home_on, away_on = set(), set()
        
        for ev in raw_events:
            if not isinstance(ev, dict): continue
            
            act_id = str(ev.get('idAction', 'UNK'))
            t_str = ev.get('cronometer', "00:00")
            is_loc = ev.get('local')
            loc = "HOME" if is_loc is True else "AWAY"
            
            pname = ev.get('player', {}).get('nickName')
            pid = str(ev.get('player', {}).get('id', '')).replace("P","").strip()
            
            # Gestión Cambios
            if pname:
                target = home_on if loc == "HOME" else away_on
                if act_id in ['599', '112']: target.add((pname, pid))
                elif act_id == '115': 
                    target = {x for x in target if x[0] != pname}
                    if loc == "HOME": home_on = target
                    else: away_on = target
                else: target.add((pname, pid))
            
            # Guardamos estado
            h_list = sorted(list(home_on))[:5]
            a_list = sorted(list(away_on))[:5]
            
            # Seconds
            m, s = 0, 0
            if ':' in t_str: m,s = map(int, t_str.split(':'))
            secs = m*60 + s
            
            processed_rows.append({
                'GameID': gid, 'Season': season, 'Week': week,
                'Period': ev.get('period'), 'Seconds': secs, 'Time': t_str,
                'Score_Home': ev.get('homeScore', 0), 'Score_Away': ev.get('awayScore', 0),
                'H_Lineup': tuple([x[0] for x in h_list]), 'A_Lineup': tuple([x[0] for x in a_list]),
                'H_IDs': tuple([x[1] for x in h_list]), 'A_IDs': tuple([x[1] for x in a_list])
            })
        return processed_rows
    except: return []

def calculate_stats_from_rows(rows):
    """Calcula estadísticas de una lista de eventos procesados."""
    if not rows: return []
    
    df = pd.DataFrame(rows)
    # Forward Fill
    cols = ['H_Lineup','A_Lineup','H_IDs','A_IDs']
    for c in cols:
        df[c] = df[c].apply(lambda x: x if x and len(x) == 5 else None).ffill()
    
    stats = {}
    
    for gid, df_g in df.groupby('GameID'):
        df_g = df_g.sort_index()
        prev_h, prev_a = 0, 0
        
        for i in range(len(df_g)):
            row = df_g.iloc[i]
            dur = 0
            if i < len(df_g)-1:
                if row['Period'] == df_g.iloc[i+1]['Period']:
                    dur = max(0, row['Seconds'] - df_g.iloc[i+1]['Seconds'])
            
            # Acumular tiempo y puntos
            diff_h = row['Score_Home'] - prev_h
            diff_a = row['Score_Away'] - prev_a
            
            # HOME
            if row['H_Lineup']:
                k = (gid, "HOME", row['H_Lineup'], row['H_IDs'])
                if k not in stats: stats[k] = {'Sec':0, 'PF':0, 'PA':0}
                s = stats[k]
                s['Sec'] += dur
                if diff_h > 0: s['PF'] += diff_h
                if diff_a > 0: s['PA'] += diff_a
            
            # AWAY
            if row['A_Lineup']:
                k = (gid, "AWAY", row['A_Lineup'], row['A_IDs'])
                if k not in stats: stats[k] = {'Sec':0, 'PF':0, 'PA':0}
                s = stats[k]
                s['Sec'] += dur
                if diff_a > 0: s['PF'] += diff_a
                if diff_h > 0: s['PA'] += diff_h
                
            prev_h = row['Score_Home']; prev_a = row['Score_Away']
            
    # Formato final
    final_data = []
    for (gid, loc, names, ids), val in stats.items():
        if val['Sec'] > 0:
            d = {
                'GameID': gid, 'Season': CURRENT_SEASON, 'Team': 'UNK', 'Location': loc,
                'Week': df.iloc[0]['Week'],
                'Time': f"{int(val['Sec']//60):02d}:{int(val['Sec']%60):02d}",
                'Seconds': val['Sec'], 'PF': val['PF'], 'PA': val['PA'], 
                'PlusMinus': val['PF'] - val['PA']
            }
            for idx, (name, pid) in enumerate(zip(names, ids)):
                d[f"J{idx+1}"] = name; d[f"ID{idx+1}"] = pid
            final_data.append(d)
    return final_data

# ==============================================================================
# 3. INTERFAZ
# ==============================================================================

st.title(f"🏀 Super Scout ACB {CURRENT_SEASON}")

with st.expander("🔄 SINCRONIZAR DATOS", expanded=True):
    if st.button("BUSCAR Y ACTUALIZAR", type="primary"):
        status_box = st.container()
        
        with status_box:
            # 1. Comprobar existentes
            existing = get_existing_games_in_bq()
            st.info(f"💾 Partidos ya en Nube: {len(existing)}")
            
            # 2. Comprobar Web
            df_web = get_played_games_on_web()
            if df_web.empty:
                st.error("Error conectando con ACB.com")
            else:
                web_ids = set(df_web['id'].tolist())
                missing = web_ids - existing
                
                if not missing:
                    st.success("✅ Todo actualizado.")
                else:
                    st.warning(f"⚡ Faltan {len(missing)} partidos. Descargando por lotes...")
                    
                    # --- PROCESO POR LOTES (BATCHING) ---
                    # Esto evita que se pete la memoria
                    missing_list = df_web[df_web['id'].isin(missing)].to_dict('records')
                    BATCH_SIZE = 10 # Procesamos de 10 en 10
                    
                    progress_bar = st.progress(0)
                    total_batches = (len(missing_list) // BATCH_SIZE) + 1
                    
                    for i in range(0, len(missing_list), BATCH_SIZE):
                        batch = missing_list[i : i + BATCH_SIZE]
                        current_batch_num = (i // BATCH_SIZE) + 1
                        
                        st.write(f"📦 Procesando Lote {current_batch_num}/{total_batches} ({len(batch)} partidos)...")
                        
                        batch_data = []
                        for game in batch:
                            rows = process_single_game(game['id'], CURRENT_SEASON, game['Week'])
                            if rows: batch_data.extend(calculate_stats_from_rows(rows))
                        
                        # Subir Lote inmediatamente
                        if batch_data:
                            df_batch = pd.DataFrame(batch_data)
                            pandas_gbq.to_gbq(
                                df_batch, f"{DATASET_ID}.lineups", project_id=PROJECT_ID, 
                                if_exists='append', credentials=credentials
                            )
                            st.write(f"   ✅ Lote {current_batch_num} subido a la nube.")
                        
                        progress_bar.progress(min((i + BATCH_SIZE) / len(missing_list), 1.0))
                    
                    st.balloons()
                    st.success("🎉 ¡PROCESO TERMINADO! Recargando...")
                    time.sleep(2)
                    st.rerun()

st.divider()

# --- VISOR ---
# (Código del visor igual que antes, optimizado)
q = f"SELECT DISTINCT GameID, Week, Location FROM `{PROJECT_ID}.{DATASET_ID}.lineups` WHERE Season = '{CURRENT_SEASON}' ORDER BY GameID DESC"
try: df_idx = client.query(q).to_dataframe()
except: df_idx = pd.DataFrame()

if not df_idx.empty:
    jornadas = sorted(df_idx['Week'].unique(), key=lambda x: int(x))
    sel_week = st.selectbox("Jornada", jornadas, index=len(jornadas)-1)
    df_g = df_idx[df_idx['Week'] == sel_week]
    df_g['L'] = df_g.apply(lambda x: f"ID {x['GameID']} ({x['Location']})", axis=1)
    sel_g = st.selectbox("Partido", df_g['L'].unique())
    gid = df_g[df_g['L'] == sel_g]['GameID'].iloc[0]
    
    q2 = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.lineups` WHERE GameID = {gid} ORDER BY Seconds DESC"
    df_data = client.query(q2).to_dataframe()
    
    def make_pretty_table(df):
        def get_img(pid, name):
            if not pid or pid == "": return f"<div style='font-size:10px;'>{name[:3]}</div>"
            cid = str(pid).replace(".0","").strip()
            return f"<div style='text-align:center;'><img src='https://static.acb.com/img/jugadores/JPG/{cid}.jpg' style='width:40px;height:40px;border-radius:50%;object-fit:cover;' onerror=this.src='https://via.placeholder.com/40'><br><span style='font-size:9px;'>{name}</span></div>"

        html = "<table style='width:100%;font-size:12px;border-collapse:collapse;'>"
        html += "<tr style='background:#333;color:#fff;'><th>QUINTETO</th><th>MIN</th><th>+/-</th><th>PTS</th><th>RIV</th></tr>"
        for _, r in df.iterrows():
            bg = "#eaffea" if r['PlusMinus'] > 0 else "#ffeaea"
            col = "green" if r['PlusMinus'] > 0 else "red"
            p_html = "<div style='display:flex;gap:4px;'>" + "".join([get_img(r[f'ID{i}'], r[f'J{i}']) for i in range(1,6)]) + "</div>"
            html += f"<tr style='background:{bg};border-bottom:1px solid #ddd;'><td style='padding:4px;'>{p_html}</td><td style='text-align:center;'>{r['Time']}</td><td style='text-align:center;font-weight:bold;color:{col};font-size:14px;'>{int(r['PlusMinus'])}</td><td style='text-align:center;'>{r['PF']}</td><td style='text-align:center;'>{r['PA']}</td></tr>"
        return html + "</table>"
    
    st.markdown(make_pretty_table(df_data), unsafe_allow_html=True)
else:
    st.info("Base de datos vacía. Sincroniza arriba.")
