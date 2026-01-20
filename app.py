import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import plotly.express as px
import math
import io
import warnings
from database import get_connection, get_placeholder, execute_insert_returning_id
from poi_data import POIS_NUPORANGA
import functools

# Ignorar avisos
warnings.filterwarnings("ignore")

st.set_page_config(page_title="Gestão de Frota", layout="wide")
st.title("🚛 Gestão de Frota")

# Funções com cache para melhorar performance
@st.cache_data(ttl=60)  # Cache por 60 segundos
def get_veiculos():
    """Busca lista de veículos com cache"""
    conn = get_connection()
    query_v = """
        SELECT DISTINCT p.id_veiculo, v.placa 
        FROM posicoes_raw p 
        LEFT JOIN veiculos v ON p.id_veiculo = v.id_sascar
        ORDER BY v.placa, p.id_veiculo
    """
    df = pd.read_sql(query_v, conn)
    conn.close()
    return df

@st.cache_data(ttl=30)  # Cache por 30 segundos
def get_deslocamentos_pendentes(placa):
    """Busca deslocamentos pendentes com cache"""
    conn = get_connection()
    query = f"""
        SELECT id, data_inicio, data_fim, km_inicial, km_final, distancia,
               local_inicio, local_fim, tempo, tempo_ocioso, situacao
        FROM deslocamentos 
        WHERE placa = {get_placeholder(1)} AND status = 'PENDENTE'
        ORDER BY data_inicio
    """
    df = pd.read_sql(query, conn, params=(placa,))
    conn.close()
    return df

@st.cache_data(ttl=30)
def get_placas_pendentes():
    """Busca placas com deslocamentos pendentes com cache"""
    conn = get_connection()
    query = """
        SELECT DISTINCT placa FROM deslocamentos 
        WHERE status = 'PENDENTE' 
        ORDER BY placa
    """
    try:
        placas = pd.read_sql(query, conn)['placa'].tolist()
    except:
        placas = []
    conn.close()
    return placas

@st.cache_data(ttl=60)
def get_ultimas_posicoes():
    """Busca últimas posições de todos os veículos com cache"""
    conn = get_connection()
    query = """
        SELECT 
            p.id_veiculo,
            v.placa,
            p.latitude,
            p.longitude,
            p.data_hora,
            p.ignicao,
            p.velocidade,
            p.odometro
        FROM posicoes_raw p
        LEFT JOIN veiculos v ON p.id_veiculo = v.id_sascar
        WHERE p.id_veiculo IN (
            SELECT DISTINCT id_veiculo FROM posicoes_raw
        )
        AND p.data_hora = (
            SELECT MAX(data_hora) 
            FROM posicoes_raw 
            WHERE id_veiculo = p.id_veiculo
        )
        ORDER BY v.placa, p.id_veiculo
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

@st.cache_data(ttl=30)
def get_viagens_historico():
    """Busca histórico de viagens com cache"""
    conn = get_connection()
    query = """
        SELECT id, placa, data_inicio, data_fim, operacao, rota, num_cte, 
               valor, distancia_total, tipo_viagem, observacao
        FROM viagens 
        ORDER BY data_inicio DESC
        LIMIT 100
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Carregar veículos
veiculos_df = get_veiculos()

if veiculos_df.empty:
    st.error("Nenhum dado de GPS encontrado no banco de dados.")
    st.stop()

# Criar lista para exibição: "Placa (ID)" ou apenas "ID" if not mapped
veiculos_df['display'] = veiculos_df.apply(
    lambda x: f"{x['placa']} ({x['id_veiculo']})" if pd.notnull(x['placa']) else f"ID: {x['id_veiculo']} (Não Cadastrado)", 
    axis=1
)

# --- CONFIGURAÇÃO ---
# (Sem sidebar complexo mais)


# Configurações padrão (internos, sem controle manual)
raio_base = 3000
raio_poi = 1000
intervalo_dias = st.sidebar.slider("Janela de Análise (Dias)", 1, 30, 7)

# --- NAVEGAÇÃO PRINCIPAL ---
st.header("Navegação")
tab_overview, tab_fechamento, tab_historico = st.tabs(["🗺️ Visão Geral da Frota", "📋 Fechamento de Viagens", "📚 Histórico & Gestão"])

# ========================================
# TAB 1: VISÃO GERAL DA FROTA
# ========================================
with tab_overview:
    st.subheader("Última Posição de Todos os Veículos")
    
    # Buscar última posição de cada veículo (com cache)
    df_ultimas = get_ultimas_posicoes()
    
    if df_ultimas.empty:
        st.warning("Nenhuma posição GPS encontrada no banco de dados.")
    else:
        st.info(f"Mostrando {len(df_ultimas)} veículos ativos")
        
        # Criar mapa centrado na média das posições
        lat_centro = df_ultimas['latitude'].mean()
        lon_centro = df_ultimas['longitude'].mean()
        
        mapa_geral = folium.Map(location=[lat_centro, lon_centro], zoom_start=7)
        
        # Adicionar marcador para cada veículo
        for idx, row in df_ultimas.iterrows():
            placa_display = row['placa'] if pd.notnull(row['placa']) else f"ID {row['id_veiculo']}"
            
            # Determinar cor do marcador baseado em ignição
            cor = 'green' if row['ignicao'] == 1 else 'red'
            icone = 'play' if row['ignicao'] == 1 else 'stop'
            
            # Formatar hora
            try:
                data_hora = pd.to_datetime(row['data_hora'])
                hora_str = data_hora.strftime('%d/%m %H:%M')
            except:
                hora_str = str(row['data_hora'])
            
            # Popup com informações
            popup_html = f"""
            <b>{placa_display}</b><br>
            🕐 {hora_str}<br>
            ⚡ {'Ligado' if row['ignicao'] == 1 else 'Desligado'}<br>
            🚗 {row['velocidade']:.0f} km/h<br>
            📍 {row['odometro']:.1f} km
            """
            
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_html, max_width=200),
                tooltip=placa_display,
                icon=folium.Icon(color=cor, icon=icone, prefix='fa')
            ).add_to(mapa_geral)
        
        # Renderizar mapa
        st_folium(mapa_geral, height=600, width=None)
        
        # Tabela resumo
        st.subheader("Resumo das Últimas Posições")
        df_display = df_ultimas.copy()
        df_display['Placa'] = df_display.apply(
            lambda x: x['placa'] if pd.notnull(x['placa']) else f"ID {x['id_veiculo']}", axis=1
        )
        df_display['Última Atualização'] = pd.to_datetime(df_display['data_hora']).dt.strftime('%d/%m/%Y %H:%M')
        df_display['Status'] = df_display['ignicao'].apply(lambda x: '🟢 Ligado' if x == 1 else '🔴 Desligado')
        df_display['Velocidade'] = df_display['velocidade'].apply(lambda x: f"{x:.0f} km/h")
        df_display['Odômetro'] = df_display['odometro'].apply(lambda x: f"{x:.1f} km")
        
        st.dataframe(
            df_display[['Placa', 'Última Atualização', 'Status', 'Velocidade', 'Odômetro']],
            use_container_width=True
        )

# ========================================
# TAB 2: ANÁLISE INDIVIDUAL
# ========================================
# TAB 3: FECHAMENTO DE VIAGENS
# ========================================
with tab_fechamento:
    st.subheader("📋 Fechamento Manual de Viagens")
    st.markdown("""
    Selecione deslocamentos pendentes e agrupe-os em viagens, classificando conforme as regras de negócio.
    """)
    
    # Buscar placas com deslocamentos pendentes (com cache)
    placas_pendentes = get_placas_pendentes()
    
    if not placas_pendentes:
        st.info("✅ Não há deslocamentos pendentes para processar.")
        st.markdown("**Dica:** Execute o processador de deslocamentos para identificar novos trechos.")
        
        # Botão para executar processador
        if st.button("🔄 Processar Deslocamentos", key="btn_processar"):
            with st.spinner("Processando posições brutas..."):
                try:
                    from processor import processar_deslocamentos
                    processar_deslocamentos()
                    st.success("Processamento concluído! Recarregue a página para ver os resultados.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao processar: {e}")
    else:
        # Seleção de Placa
        placa_selecionada = st.selectbox(
            "🚛 Selecione a Placa:",
            placas_pendentes,
            key="placa_fechamento"
        )
        
        if placa_selecionada:
            # Buscar deslocamentos pendentes desta placa (com cache)
            df_desloc = get_deslocamentos_pendentes(placa_selecionada)
            
            if df_desloc.empty:
                st.warning("Nenhum deslocamento pendente para esta placa.")
            else:
                st.markdown(f"**{len(df_desloc)} deslocamentos pendentes**")
                
                # Formatar para exibição
                df_display = df_desloc.copy()
                df_display['Data Início'] = pd.to_datetime(df_display['data_inicio']).dt.strftime('%d/%m/%Y %H:%M')
                df_display['Data Fim'] = pd.to_datetime(df_display['data_fim']).dt.strftime('%d/%m/%Y %H:%M')
                df_display['Distância'] = df_display['distancia'].apply(lambda x: f"{x:.1f} km" if x else "0 km")
                
                # Calcular duração real a partir das datas (diferença entre data_fim e data_inicio)
                def calc_duration(row):
                    try:
                        inicio = pd.to_datetime(row['data_inicio'])
                        fim = pd.to_datetime(row['data_fim'])
                        diff_minutes = (fim - inicio).total_seconds() / 60
                        h, m = divmod(int(diff_minutes), 60)
                        return f"{h:02d}:{m:02d}"
                    except:
                        return "00:00"
                
                # Formatar tempo ocioso como HH:MM
                def format_minutes(mins):
                    if pd.isna(mins) or mins == 0:
                        return "00:00"
                    h, m = divmod(int(mins), 60)
                    return f"{h:02d}:{m:02d}"
                
                df_display['Tempo'] = df_desloc.apply(calc_duration, axis=1)
                df_display['Parado'] = df_display['tempo_ocioso'].apply(format_minutes)
                df_display['Situação'] = df_display['situacao'].apply(lambda x: '⏸ PARADO' if x == 'PARADO' else '▶ MOVIMENTO')
                df_display['Selecionar'] = False
                
                # Data editor para seleção
                cols_editor = ['Selecionar', 'Situação', 'Data Início', 'Data Fim', 'Tempo', 'Parado', 'local_inicio', 'local_fim', 'Distância']
                df_edit = st.data_editor(
                    df_display[cols_editor],
                    column_config={
                        "Selecionar": st.column_config.CheckboxColumn("✓", default=False, width="small"),
                        "Situação": st.column_config.TextColumn("Situação", width="small"),
                        "Tempo": st.column_config.TextColumn("Duração", width="small"),
                        "Parado": st.column_config.TextColumn("Motor Lig.", width="small"),
                        "local_inicio": st.column_config.TextColumn("Local Início", width="medium"),
                        "local_fim": st.column_config.TextColumn("Local Fim", width="medium"),
                    },
                    width="stretch",
                    key="editor_deslocamentos"
                )
                
                # Obter IDs selecionados
                selected_mask = df_edit['Selecionar']
                selected_ids = df_desloc[selected_mask]['id'].tolist()
                
                st.divider()
                
                # Formulário de Validação
                if selected_ids:
                    st.markdown(f"### 📝 Validar {len(selected_ids)} deslocamento(s) selecionado(s)")
                    
                    # Calcular totais consolidados
                    df_selected = df_desloc[df_desloc['id'].isin(selected_ids)]
                    data_inicio_viagem = df_selected['data_inicio'].min()
                    data_fim_viagem = df_selected['data_fim'].max()
                    # Converter para float nativo Python (evita erro np.float64 no PostgreSQL)
                    distancia_total = float(df_selected['distancia'].sum())
                    tempo_total = float(df_selected['tempo'].sum()) if 'tempo' in df_selected.columns else 0.0
                    tempo_parado = float(df_selected['tempo_ocioso'].sum()) if 'tempo_ocioso' in df_selected.columns else 0.0
                    
                    # Formatar tempos
                    def fmt_hhmm(mins):
                        h, m = divmod(int(mins), 60)
                        return f"{h:02d}:{m:02d}"
                    
                    # Mostrar resumo
                    col_res1, col_res2, col_res3, col_res4, col_res5 = st.columns(5)
                    with col_res1:
                        st.metric("Período", f"{pd.to_datetime(data_inicio_viagem).strftime('%d/%m %H:%M')} - {pd.to_datetime(data_fim_viagem).strftime('%d/%m %H:%M')}")
                    with col_res2:
                        st.metric("Tempo Total", fmt_hhmm(tempo_total))
                    with col_res3:
                        st.metric("Motor Parado", fmt_hhmm(tempo_parado))
                    with col_res4:
                        st.metric("Distância", f"{distancia_total:.1f} km")
                    with col_res5:
                        st.metric("Trechos", str(len(selected_ids)))
                    
                    
                    # --- Gestão de CTEs (Helper Dinâmico) ---
                    st.markdown("##### 📄 Documentos Fiscais (CTEs)")
                    if 'ctes' not in st.session_state: st.session_state.ctes = []

                    def add_cte():
                        val = st.session_state.get('new_cte_input', '').strip()
                        if val:
                            if val not in st.session_state.ctes:
                                st.session_state.ctes.append(val)
                            st.session_state.new_cte_input = "" # Limpar input

                    c_cte1, c_cte2 = st.columns([3, 1])
                    c_cte1.text_input("Novo CTE", key="new_cte_input", placeholder="Digite e clique no +", label_visibility="collapsed")
                    c_cte2.button("➕ Adicionar", on_click=add_cte, use_container_width=True)

                    # Listar
                    if st.session_state.ctes:
                        st.caption("CTEs incluídos:")
                        # Chips removíveis
                        cols = st.columns(4)
                        for i, cte in enumerate(st.session_state.ctes):
                            if cols[i % 4].button(f"🗑️ {cte}", key=f"del_cte_{i}", help="Clique para remover"):
                                st.session_state.ctes.pop(i)
                                st.rerun()
                    else:
                        st.info("Nenhum CTE informado.")

                    with st.form("form_viagem"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            operacao = st.selectbox(
                                "Operação *",
                                ["Ovos - Tatui", "Frango - Passos", "Pintos - Ipigua", 
                                 "Ovos - Nuporanga", "Pintos - Nuporanga", "Apoio", "Ovos - Nova Mutum"],
                                key="operacao"
                            )
                            
                            rota = st.text_input("Rota", placeholder="Ex: Base > Granja X > Base", key="rota")
                            
                            # Campo de CTE agora reflete a lista
                            cte_str = ", ".join(st.session_state.ctes)
                            num_cte = st.text_input("Nº CTEs (Use os botões acima)", value=cte_str, disabled=True, key="cte_final_display")
                        
                        with col2:
                            valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, key="valor")
                            
                            tipo_viagem = st.radio(
                                "Tipo de Viagem *",
                                ["PRODUTIVA", "IMPRODUTIVA"],
                                horizontal=True,
                                key="tipo_viagem"
                            )
                            
                            if tipo_viagem == "IMPRODUTIVA":
                                motivo_improd = st.selectbox(
                                    "Motivo",
                                    ["Manutenção", "Vazio", "Desvio", "Outro"],
                                    key="motivo_improd"
                                )
                        
                        observacao = st.text_area("Observação", placeholder="Observações adicionais...", key="obs")
                        
                        col_btn1, col_btn2 = st.columns(2)
                        
                        with col_btn1:
                            submitted = st.form_submit_button("✅ Confirmar Viagem", use_container_width=True, type="primary")
                        
                        with col_btn2:
                            marcar_improd = st.form_submit_button("⚠️ Marcar como Improdutivo", use_container_width=True)
                    
                    # Processar confirmação
                    if submitted:
                        try:
                            conn = get_connection()
                            c = conn.cursor()
                            
                            # Construir rota automática se não preenchida
                            if not rota:
                                locais = df_selected['local_inicio'].tolist() + [df_selected['local_fim'].iloc[-1]]
                                rota = " > ".join([l for l in locais if l and l != "Em Trânsito"])
                            
                            # Observação com motivo se improdutiva
                            obs_final = observacao
                            if tipo_viagem == "IMPRODUTIVA" and 'motivo_improd' in dir():
                                obs_final = f"[{motivo_improd}] {observacao}".strip()
                            
                            # Inserir viagem
                            ph_ins = get_placeholder(12)
                            sql_ins = f"""
                                INSERT INTO viagens 
                                (placa, data_inicio, data_fim, tempo_total, tempo_parado, operacao, rota, num_cte, valor, distancia_total, tipo_viagem, observacao)
                                VALUES ({ph_ins})
                            """
                            params_ins = (
                                placa_selecionada,
                                str(data_inicio_viagem) if hasattr(data_inicio_viagem, 'isoformat') else data_inicio_viagem,
                                str(data_fim_viagem) if hasattr(data_fim_viagem, 'isoformat') else data_fim_viagem,
                                float(tempo_total),
                                float(tempo_parado),
                                operacao,
                                rota,
                                num_cte,
                                float(valor) if valor else 0.0,
                                float(distancia_total),
                                tipo_viagem,
                                obs_final
                            )
                            
                            # Obter ID da viagem recém criada
                            viagem_id = execute_insert_returning_id(c, sql_ins, params_ins)
                            
                            # Atualizar status e viagem_id dos deslocamentos
                            ph_vid = get_placeholder(1)
                            ph_ids = get_placeholder(len(selected_ids))
                            c.execute(f"UPDATE deslocamentos SET status = 'PROCESSADO', viagem_id = {ph_vid} WHERE id IN ({ph_ids})", [viagem_id] + selected_ids)
                            
                            conn.commit()
                            conn.close()
                            
                            st.success(f"✅ Viagem registrada com sucesso! {len(selected_ids)} deslocamentos processados.")
                            
                            # Limpar lista de CTEs
                            if 'ctes' in st.session_state:
                                del st.session_state.ctes
                            
                            # Limpar cache e recarregar
                            st.cache_data.clear()
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Erro ao salvar viagem: {e}")
                    
                    # Processar marcação como improdutivo (individual ou múltiplo)
                    if marcar_improd:
                        try:
                            conn = get_connection()
                            c = conn.cursor()
                            
                            # Criar viagens individuais para cada deslocamento como improdutivo
                            for _, row in df_selected.iterrows():
                                tempo_desloc = float(row.get('tempo', 0)) if 'tempo' in row else 0.0
                                tempo_parado_desloc = float(row.get('tempo_ocioso', 0)) if 'tempo_ocioso' in row else 0.0
                                sql_imp = f"""
                                    INSERT INTO viagens 
                                    (placa, data_inicio, data_fim, tempo_total, tempo_parado, operacao, rota, num_cte, valor, distancia_total, tipo_viagem, observacao)
                                    VALUES ({get_placeholder(12)})
                                """
                                params_imp = (
                                    placa_selecionada,
                                    str(row['data_inicio']) if hasattr(row['data_inicio'], 'isoformat') else row['data_inicio'],
                                    str(row['data_fim']) if hasattr(row['data_fim'], 'isoformat') else row['data_fim'],
                                    float(tempo_desloc),
                                    float(tempo_parado_desloc),
                                    "Apoio",
                                    f"{row['local_inicio']} > {row['local_fim']}",
                                    "",
                                    0.0,
                                    float(row['distancia']) if row['distancia'] else 0.0,
                                    "IMPRODUTIVA",
                                    observacao if observacao else "Marcado como improdutivo"
                                )
                                
                                # Obter ID da viagem e vincular ao deslocamento
                                viagem_id = execute_insert_returning_id(c, sql_imp, params_imp)
                                ph_u = get_placeholder(1)
                                c.execute(f"UPDATE deslocamentos SET status = 'PROCESSADO', viagem_id = {ph_u} WHERE id = {ph_u}", (viagem_id, row['id']))
                            
                            conn.commit()
                            conn.close()
                            
                            st.success(f"⚠️ {len(selected_ids)} deslocamento(s) marcado(s) como improdutivo(s).")
                            # Limpar cache e recarregar
                            st.cache_data.clear()
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Erro ao marcar como improdutivo: {e}")
                
                else:
                    st.info("👆 Selecione um ou mais deslocamentos acima para criar uma viagem.")
        
        # Histórico movido para tab própria
        
# ========================================
# TAB 3: HISTÓRICO & GESTÃO
# ========================================
with tab_historico:
    st.subheader("📚 Histórico de Viagens")
    st.markdown("Visualize as viagens fechadas e exclua se necessário (deslocamentos voltam a ser Pendentes).")
    
    # Seção de Exclusão
    with st.expander("🗑️ Excluir Viagem", expanded=False):
        col_del1, col_del2 = st.columns([1, 2])
        with col_del1:
            v_id_del = st.number_input("ID da Viagem para excluir", min_value=1, step=1)
        with col_del2:
            st.write("") # Spacer
            st.write("")
            if st.button("⚠️ Excluir Viagem e Liberar Deslocamentos"):
                conn = get_connection()
                c = conn.cursor()
                # Verificar se existe
                ph_ex = get_placeholder(1)
                c.execute(f"SELECT id FROM viagens WHERE id = {ph_ex}", (v_id_del,))
                if not c.fetchone():
                    st.error(f"Viagem {v_id_del} não encontrada.")
                    conn.close()
                else:
                    try:
                        # 1. Liberar deslocamentos
                        c.execute(f"UPDATE deslocamentos SET status='PENDENTE', viagem_id=NULL WHERE viagem_id = {ph_ex}", (v_id_del,))
                        qtd_liberada = c.rowcount
                        
                        # 2. Excluir viagem
                        c.execute(f"DELETE FROM viagens WHERE id = {ph_ex}", (v_id_del,))
                        conn.commit()
                        st.success(f"✅ Viagem {v_id_del} excluída! {qtd_liberada} deslocamentos voltaram para 'PENDENTE'.")
                        # Limpar cache e recarregar
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao excluir: {e}")
                    finally:
                        conn.close()
    
    st.divider()
    
    # Listagem (com cache)
    try:
        df_viagens = get_viagens_historico()
        
        if df_viagens.empty:
            st.info("Nenhuma viagem registrada ainda.")
        else:
            df_viagens['Data Início'] = pd.to_datetime(df_viagens['data_inicio']).dt.strftime('%d/%m/%Y %H:%M')
            df_viagens['Data Fim'] = pd.to_datetime(df_viagens['data_fim']).dt.strftime('%d/%m/%Y %H:%M')
            df_viagens['Valor'] = df_viagens['valor'].apply(lambda x: f"R$ {x:.2f}" if x else "-")
            df_viagens['Distância'] = df_viagens['distancia_total'].apply(lambda x: f"{x:.1f} km" if x else "-")
            
            # Colorir por tipo
            def highlight_tipo(row):
                if row['tipo_viagem'] == 'PRODUTIVA':
                    return ['background-color: #d4edda'] * len(row)
                else:
                    return ['background-color: #f8d7da'] * len(row)
            
            cols_viagens = ['id', 'placa', 'Data Início', 'Data Fim', 'operacao', 'rota', 'Distância', 'Valor', 'tipo_viagem', 'observacao']
            
            st.dataframe(
                df_viagens[cols_viagens].style.apply(highlight_tipo, axis=1),
                use_container_width=True,
                hide_index=True
            )
    except Exception as e:
        st.error(f"Erro ao carregar histórico: {e}")
