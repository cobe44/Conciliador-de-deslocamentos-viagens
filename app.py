"""
LOGLIVE Console - Final V2
===========================
Fixed: numpy types, origin/destination, motor time format, strict stop logic
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import uuid

st.set_page_config(page_title="LOGLIVE", layout="wide", initial_sidebar_state="expanded")
st.markdown("<style>.block-container{padding-top:0.5rem!important}#MainMenu,footer,header{visibility:hidden}</style>", unsafe_allow_html=True)

from sqlalchemy import create_engine, text
from database import DATABASE_URL
engine = create_engine(DATABASE_URL)

# Ensure tables
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS deslocamentos (
            id VARCHAR(100) PRIMARY KEY, placa VARCHAR(20) NOT NULL, truck_id INTEGER NOT NULL,
            tipo VARCHAR(20) NOT NULL, data_inicio TIMESTAMP NOT NULL, data_fim TIMESTAMP NOT NULL,
            duracao_min FLOAT DEFAULT 0, dist_km FLOAT DEFAULT 0, motor_ligado_min FLOAT DEFAULT 0,
            lat_inicio FLOAT, lon_inicio FLOAT, lat_fim FLOAT, lon_fim FLOAT,
            local_inicio VARCHAR(100), local_fim VARCHAR(100),
            validado BOOLEAN DEFAULT FALSE, trip_id VARCHAR(100), 
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS final_trips (
            id VARCHAR(100) PRIMARY KEY, placa VARCHAR(20), truck_id INTEGER,
            data_inicio TIMESTAMP, data_fim TIMESTAMP, origem VARCHAR(100), destino VARCHAR(100),
            km_total FLOAT DEFAULT 0, tempo_mov_min FLOAT DEFAULT 0, tempo_par_min FLOAT DEFAULT 0,
            motorista VARCHAR(100), cte VARCHAR(50), valor FLOAT DEFAULT 0,
            tipo VARCHAR(50) DEFAULT 'Produtiva', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.commit()


def fmt_min(m):
    """Format minutes as hh:mm"""
    m = float(m) if m else 0
    h = int(m // 60)
    mm = int(m % 60)
    return f"{h}h{mm:02d}m" if h > 0 else f"{mm}m"


@st.cache_data(ttl=300)
def load_plates():
    return pd.read_sql("SELECT DISTINCT placa FROM veiculos WHERE placa IS NOT NULL ORDER BY placa", engine)['placa'].tolist()


def load_events(placa):
    return pd.read_sql(f"SELECT * FROM deslocamentos WHERE placa = '{placa}' AND validado = FALSE ORDER BY data_inicio", engine)


def load_trips(placa):
    return pd.read_sql(f"SELECT * FROM final_trips WHERE placa = '{placa}' ORDER BY data_inicio DESC", engine)


def load_route(truck_id, t0, t1):
    return pd.read_sql(f"SELECT latitude, longitude FROM posicoes_raw WHERE id_veiculo = {truck_id} AND data_hora BETWEEN '{t0}' AND '{t1}' ORDER BY data_hora", engine)


def process_plate(placa, dias=7):
    from processor import process_single
    return process_single(placa, dias)


def save_trip(data, event_ids):
    try:
        tid = str(uuid.uuid4())
        
        # Convert numpy types to Python native
        km = float(data['km']) if hasattr(data['km'], 'item') else float(data['km'])
        mov = float(data['mov']) if hasattr(data['mov'], 'item') else float(data['mov'])
        par = float(data['par']) if hasattr(data['par'], 'item') else float(data['par'])
        val = float(data.get('valor', 0)) if data.get('valor') else 0.0
        
        # Escape strings
        origem = str(data['origem']).replace("'", "''")
        destino = str(data['destino']).replace("'", "''")
        motorista = str(data.get('motorista', '')).replace("'", "''")
        cte = str(data.get('cte', '')).replace("'", "''")
        tipo = str(data.get('tipo', 'Produtiva'))
        
        sql = f"""
            INSERT INTO final_trips (id, placa, truck_id, data_inicio, data_fim, origem, destino, 
                km_total, tempo_mov_min, tempo_par_min, motorista, cte, valor, tipo)
            VALUES ('{tid}', '{data['placa']}', {data['truck_id']}, '{data['t0']}', '{data['t1']}',
                '{origem}', '{destino}', {km}, {mov}, {par}, '{motorista}', '{cte}', {val}, '{tipo}')
        """
        
        with engine.connect() as conn:
            conn.execute(text(sql))
            for eid in event_ids:
                conn.execute(text(f"UPDATE deslocamentos SET validado=TRUE, trip_id='{tid}' WHERE id='{eid}'"))
            conn.commit()
        return True
    except Exception as e:
        st.error(f"Erro: {e}")
        return False


def delete_trip(tid):
    try:
        with engine.connect() as conn:
            conn.execute(text(f"UPDATE deslocamentos SET validado=FALSE, trip_id=NULL WHERE trip_id='{tid}'"))
            conn.execute(text(f"DELETE FROM final_trips WHERE id='{tid}'"))
            conn.commit()
        return True
    except Exception as e:
        st.error(f"Erro: {e}")
        return False


# Sidebar
st.sidebar.title("🚛 LOGLIVE")
plates = load_plates()
if not plates:
    st.warning("Nenhuma placa.")
    st.stop()

placa = st.sidebar.selectbox("Placa", plates)
dias = st.sidebar.slider("Dias", 1, 30, 7)

if st.sidebar.button("🔄 Processar"):
    with st.spinner(f"Processando {placa}..."):
        n = process_plate(placa, dias)
        st.sidebar.success(f"✓ {n} eventos")
        st.rerun()


# Tabs
tab1, tab2 = st.tabs(["📋 Timeline", "🚚 Viagens"])

with tab1:
    events = load_events(placa)
    
    if events.empty:
        st.info(f"Sem eventos. Clique 'Processar' para {placa}.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Eventos", len(events))
        c2.metric("KM", f"{events['dist_km'].sum():.1f}")
        c3.metric("Movimento", len(events[events['tipo']=='DESLOCAMENTO']))
        c4.metric("Parada", len(events[events['tipo']=='PARADA']))
        
        st.divider()
        col_tl, col_map = st.columns([6, 4])
        
        with col_tl:
            st.subheader(f"Timeline: {placa}")
            df = events.copy()
            df['Sel'] = False
            df['Início'] = pd.to_datetime(df['data_inicio']).dt.strftime('%d/%m %H:%M')
            df['Fim'] = pd.to_datetime(df['data_fim']).dt.strftime('%H:%M')
            df['Duração'] = df['duracao_min'].apply(fmt_min)
            df['KM'] = df['dist_km'].apply(lambda x: f"{x:.1f}" if x > 0 else "-")
            df['Motor'] = df['motor_ligado_min'].apply(fmt_min)
            df['Tipo'] = df['tipo'].map({'DESLOCAMENTO': 'Movimento', 'PARADA': 'Parada', 'PERDA_SINAL': 'Perda Sinal'})
            
            # Handle missing columns gracefully
            if 'local_inicio' not in df.columns:
                df['local_inicio'] = df.get('local_nome', '-')
            if 'local_fim' not in df.columns:
                df['local_fim'] = df.get('local_nome', '-')
            
            edited = st.data_editor(
                df[['Sel', 'Tipo', 'Início', 'Fim', 'Duração', 'KM', 'Motor', 'local_inicio', 'local_fim']],
                column_config={
                    "Sel": st.column_config.CheckboxColumn("✓", width="small"),
                    "local_inicio": "Origem",
                    "local_fim": "Destino"
                },
                use_container_width=True, hide_index=True, height=350
            )
            
            sel = events.iloc[edited[edited['Sel']].index.tolist()] if (edited['Sel']==True).any() else pd.DataFrame()
        
        with col_map:
            st.subheader("Mapa")
            if sel.empty:
                st.info("Selecione eventos")
            else:
                truck_id = int(sel.iloc[0]['truck_id'])
                t0 = pd.to_datetime(sel['data_inicio']).min()
                t1 = pd.to_datetime(sel['data_fim']).max()
                
                fig = go.Figure()
                
                if (sel['tipo'] == 'PARADA').all():
                    # Get lat/lon - handle both old and new schema
                    if 'lat_inicio' in sel.columns:
                        lat, lon = sel.iloc[0]['lat_inicio'], sel.iloc[0]['lon_inicio']
                    else:
                        lat, lon = sel.iloc[0].get('lat', -20.5), sel.iloc[0].get('lon', -47.0)
                    
                    local = sel.iloc[0].get('local_inicio', sel.iloc[0].get('local_nome', 'Parada'))
                    fig.add_trace(go.Scattermapbox(mode="markers", lat=[lat], lon=[lon], 
                        marker={'size': 20, 'color': '#FFC107'}, text=[local]))
                    clat, clon, zoom = lat, lon, 14
                else:
                    route = load_route(truck_id, t0, t1)
                    if not route.empty:
                        fig.add_trace(go.Scattermapbox(mode="lines", lat=route['latitude'], lon=route['longitude'],
                            line={'width': 3, 'color': '#E53935'}))
                        fig.add_trace(go.Scattermapbox(mode="markers", lat=[route.iloc[0]['latitude']], 
                            lon=[route.iloc[0]['longitude']], marker={'size': 12, 'color': '#4CAF50'}))
                        fig.add_trace(go.Scattermapbox(mode="markers", lat=[route.iloc[-1]['latitude']], 
                            lon=[route.iloc[-1]['longitude']], marker={'size': 12, 'color': '#E53935'}))
                        clat = (route['latitude'].min() + route['latitude'].max()) / 2
                        clon = (route['longitude'].min() + route['longitude'].max()) / 2
                        rng = max(route['latitude'].max()-route['latitude'].min(), route['longitude'].max()-route['longitude'].min())
                        zoom = 5 if rng > 5 else 6 if rng > 2 else 7 if rng > 1 else 8 if rng > 0.5 else 10
                    else:
                        clat, clon, zoom = -20.5, -47.0, 8
                
                fig.update_layout(mapbox_style="open-street-map", 
                    mapbox=dict(center=dict(lat=clat, lon=clon), zoom=zoom),
                    margin={"r":0,"t":0,"l":0,"b":0}, height=300, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        st.subheader("📝 Compor Viagem")
        
        if sel.empty:
            st.info("Selecione eventos")
        else:
            km = float(sel['dist_km'].sum())
            t0 = pd.to_datetime(sel['data_inicio']).min()
            t1 = pd.to_datetime(sel['data_fim']).max()
            
            # Get origin/destination
            if 'local_inicio' in sel.columns:
                origem = sel.iloc[0]['local_inicio']
                destino = sel.iloc[-1]['local_fim']
            else:
                origem = sel.iloc[0].get('local_nome', '-')
                destino = sel.iloc[-1].get('local_nome', '-')
            
            mov = float(sel[sel['tipo']=='DESLOCAMENTO']['duracao_min'].sum())
            par = float(sel[sel['tipo']=='PARADA']['duracao_min'].sum())
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Eventos", len(sel))
            c2.metric("KM", f"{km:.1f}")
            c3.metric("Movimento", fmt_min(mov))
            c4.metric("Parado", fmt_min(par))
            
            st.markdown(f"**{origem}** → **{destino}**")
            
            with st.form("trip"):
                tipo = st.selectbox("Tipo", ["Produtiva", "Improdutiva", "Reposicionamento"])
                
                if tipo == "Produtiva":
                    motorista = st.text_input("Motorista")
                    cte = st.text_input("CT-e")
                    valor = st.number_input("Valor (R$)", min_value=0.0)
                else:
                    motorista, cte, valor = "", "", 0.0
                    if tipo == "Reposicionamento":
                        st.info("⚠️ Viagem de reposicionamento (sem frete)")
                
                if st.form_submit_button("✅ Salvar", type="primary"):
                    data = {'placa': placa, 'truck_id': int(sel.iloc[0]['truck_id']),
                            't0': t0, 't1': t1, 'origem': origem, 'destino': destino,
                            'km': km, 'mov': mov, 'par': par,
                            'motorista': motorista, 'cte': cte, 'valor': valor, 'tipo': tipo}
                    if save_trip(data, sel['id'].tolist()):
                        st.success("✅ Salvo!")
                        st.rerun()

with tab2:
    st.subheader("🚚 Viagens")
    trips = load_trips(placa)
    
    if trips.empty:
        st.info("Nenhuma viagem.")
    else:
        for _, t in trips.iterrows():
            with st.expander(f"{t['origem']} → {t['destino']} | {t['km_total']:.0f} km | {t['tipo']}"):
                c1, c2, c3 = st.columns(3)
                c1.write(f"**Início:** {t['data_inicio']}")
                c1.write(f"**Fim:** {t['data_fim']}")
                c2.write(f"**Tipo:** {t['tipo']}")
                c2.write(f"**Motorista:** {t['motorista'] or '-'}")
                c3.write(f"**CT-e:** {t['cte'] or '-'}")
                c3.write(f"**Valor:** R$ {t['valor']:.2f}" if t['valor'] else "")
                
                if st.button("🗑️ Excluir", key=f"del_{t['id']}"):
                    if delete_trip(t['id']):
                        st.success("Excluída!")
                        st.rerun()

st.caption(f"LOGLIVE | {placa}")
