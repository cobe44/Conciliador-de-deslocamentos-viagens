"""
Processador de Deslocamentos v5
================================
Versão com classificação baseada em VELOCIDADE:
- Processamento INCREMENTAL (sem DELETE destrutivo)
- Classificação V5: velocidade >= 3 km/h = movimento
- Consolidação automática de períodos curtos (< 5 min)
- Tratamento de período "em curso" (não insere se < 30 min)
- Rastreabilidade com raw_id_inicio e raw_id_fim
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from functools import lru_cache
from database import get_connection, get_placeholder, get_pois, migrate_db

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Usar apenas a base
from poi_data import POIS_NUPORANGA, POI_RADIUS

# Importar configurações centralizadas (com fallback se não existir)
try:
    from config import (
        VELOCIDADE_MOVIMENTO, MIN_DURACAO_PERIODO, GAP_CONSOLIDACAO,
        TEMPO_PERIODO_EM_CURSO, GAP_THRESHOLD_MINUTES, STOP_THRESHOLD_KMH,
        MIN_DISTANCIA_VIAGEM, SIGNAL_LOSS_THRESHOLD, MAX_SPEED_REALISTIC,
        TEMPO_IGN_OFF_PARADA, DIST_REINICIO_DESLOCAMENTO, BATCH_SIZE
    )
except ImportError:
    # Fallback para valores padrão se config.py não existir
    VELOCIDADE_MOVIMENTO = 3
    MIN_DURACAO_PERIODO = 5
    GAP_CONSOLIDACAO = 15
    TEMPO_PERIODO_EM_CURSO = 30
    GAP_THRESHOLD_MINUTES = 20
    STOP_THRESHOLD_KMH = 3
    MIN_DISTANCIA_VIAGEM = 2
    SIGNAL_LOSS_THRESHOLD = 60
    MAX_SPEED_REALISTIC = 150
    TEMPO_IGN_OFF_PARADA = 10
    DIST_REINICIO_DESLOCAMENTO = 3
    BATCH_SIZE = 50

# ==========================================
# GEOCODIFICAÇÃO (mantida do original)
# ==========================================
try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False


def limpar_nome_local(nome):
    """
    Limpa nomes de localização removendo prefixos verbosos.
    Ex: 'Região Geográfica Imediata de Posse-Campos' -> 'Região de Posse-Campos'
    """
    if not nome:
        return nome
    
    # Substituições para limpar nomes
    substituicoes = [
        ('Região Geográfica Imediata de ', 'Região de '),
        ('Região Geográfica Intermediária de ', 'Região de '),
        ('Microrregião de ', ''),
        ('Mesorregião de ', ''),
    ]
    
    resultado = nome
    for antigo, novo in substituicoes:
        resultado = resultado.replace(antigo, novo)
    
    return resultado.strip()


@lru_cache(maxsize=4000)
def get_cached_city_name(lat, lon):
    """
    Geocodificação reversa com cache e arredondamento para maximizar hits.
    Arredonda para 3 casas (aprox 100m) para agrupar locais próximos.
    """
    # Configuração: False = tenta API, True = apenas POIs/coordenadas
    # Reativado conforme solicitação do usuário para garantir nomes
    SKIP_GEOCODING_API = False
    GEOCODING_TIMEOUT = 3  # Timeout maior para garantir resposta
    
    try:
        lat = float(lat)
        lon = float(lon)
        lat_r = round(lat, 3)
        lon_r = round(lon, 3)
    except (ValueError, TypeError):
        return "Coordenadas Inválidas"
    
    # 1. Tentar Base Nuporanga (POIs locais) - sempre rápido
    for name, coords_list in POIS_NUPORANGA.items():
        for (p_lat, p_lon) in coords_list:
            if abs(lat_r - p_lat) < POI_RADIUS and abs(lon_r - p_lon) < POI_RADIUS:
                return name

    # 2. Se API desabilitada, retornar coordenada
    if SKIP_GEOCODING_API or not GEOPY_AVAILABLE:
        return f"{lat_r}, {lon_r}"
        
    try:
        # Geocodificação com timeout configurado
        geolocator = Nominatim(user_agent="frota_cf_v5", timeout=GEOCODING_TIMEOUT)
        loc = geolocator.reverse(f"{lat_r}, {lon_r}", language='pt')
        if loc and loc.address:
            address = loc.raw.get('address', {})
            city = address.get('city') or address.get('town') or address.get('municipality') or address.get('village')
            state = address.get('state')
            
            est_map = {
                'São Paulo': 'SP', 'Minas Gerais': 'MG', 'Goiás': 'GO', 'Paraná': 'PR',
                'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Bahia': 'BA',
                'Rio de Janeiro': 'RJ', 'Santa Catarina': 'SC', 'Rio Grande do Sul': 'RS'
            }
            uf = est_map.get(state, state) if state else ""
            
            if city:
                nome = f"{city}/{uf}" if uf and len(uf) == 2 else city
                return limpar_nome_local(nome)
            return "Em Trânsito"
            
    except Exception:
        pass
        
    return f"{lat_r}, {lon_r}"


def classificar_tipo_parada(gap_minutos, ultima_ignicao, velocidade_media_antes):
    """
    Classifica o tipo de interrupção no deslocamento.
    
    Retorna:
    - MOVIMENTO: Deslocamento normal
    - PARADA: Parada intencional (ignição desligada ou baixa velocidade)
    - PERDA_SINAL: Provável perda de sinal GPS (gap longo com ignição ligada)
    """
    if gap_minutos < GAP_THRESHOLD_MINUTES:
        return 'MOVIMENTO'
    
    # Ignição desligada = parada intencional
    if ultima_ignicao == 0:
        return 'PARADA'
    
    # Gap muito longo com ignição ligada = possível perda de sinal
    if gap_minutos > SIGNAL_LOSS_THRESHOLD:
        return 'PERDA_SINAL'
    
    # Velocidade baixa antes do gap = provavelmente parada
    if velocidade_media_antes is not None and velocidade_media_antes < STOP_THRESHOLD_KMH:
        return 'PARADA'
    
    return 'MOVIMENTO'


def calcular_tempo_ocioso(trip_df):
    """
    Calcula tempo parado (velocidade < 3km/h) dentro de um deslocamento.
    Soma os intervalos de tempo onde o veículo estava parado.
    """
    if trip_df.empty or 'time_diff' not in trip_df.columns:
        return 0.0
    
    # Pontos onde velocidade < limiar
    parado_mask = trip_df['velocidade'] < STOP_THRESHOLD_KMH
    tempo_parado = trip_df.loc[parado_mask, 'time_diff'].sum()
    
    return float(tempo_parado) if not pd.isna(tempo_parado) else 0.0


def calcular_tempo_motor_off(trip_df):
    """
    Calcula tempo total com motor desligado na viagem:
    Soma do time_diff p/ pontos onde ignicao == 0
    """
    if trip_df.empty or 'time_diff' not in trip_df.columns:
        return 0.0
        
    # Filtrar pontos com ignição 0
    pontos_off = trip_df[
        (trip_df['ignicao'] == 0) & 
        (trip_df['time_diff'].notna())
    ]
    return pontos_off['time_diff'].sum()


# ==========================================
# CLASSIFICADOR V5 - Baseado em Velocidade
# ==========================================
# Configurações V5 importadas de config.py


def classificar_deslocamentos_v5(df):
    """
    Classificador V5 - Lógica baseada em VELOCIDADE com consolidação automática
    
    Regras:
    - MOVIMENTO: velocidade >= 3 km/h
    - PARADA: velocidade < 3 km/h
    - Períodos < 5 min são consolidados com o adjacente do mesmo tipo
    - Gaps de até 15 min de parada dentro de um movimento não fragmentam
    
    Returns:
        Lista de dicionários com os períodos classificados
    """
    resultados = []
    
    for placa in df['placa'].unique():
        df_placa = df[df['placa'] == placa].sort_values('data_hora').reset_index(drop=True)
        
        if df_placa.empty:
            continue
        
        # Calcular diferenças
        df_placa['time_diff'] = df_placa['data_hora'].diff().dt.total_seconds() / 60
        
        # Classificar cada ponto como movimento ou parada baseado em velocidade
        df_placa['estado'] = df_placa['velocidade'].apply(
            lambda v: 'MOVIMENTO' if (v or 0) >= VELOCIDADE_MOVIMENTO else 'PARADA'
        )
        
        # PASSO 1: Criar períodos brutos baseados em mudança de estado
        periodos_brutos = []
        estado_atual = None
        inicio_idx = 0
        
        for idx, row in df_placa.iterrows():
            if estado_atual is None:
                estado_atual = row['estado']
                inicio_idx = idx
            elif row['estado'] != estado_atual:
                # Mudou de estado - fechar período anterior
                periodos_brutos.append({
                    'tipo': estado_atual,
                    'inicio_idx': inicio_idx,
                    'fim_idx': idx - 1,
                    'data_inicio': df_placa.loc[inicio_idx, 'data_hora'],
                    'data_fim': df_placa.loc[idx - 1, 'data_hora'],
                })
                estado_atual = row['estado']
                inicio_idx = idx
        
        # Último período
        if estado_atual is not None:
            periodos_brutos.append({
                'tipo': estado_atual,
                'inicio_idx': inicio_idx,
                'fim_idx': len(df_placa) - 1,
                'data_inicio': df_placa.loc[inicio_idx, 'data_hora'],
                'data_fim': df_placa.iloc[-1]['data_hora'],
            })
        
        # PASSO 2: Consolidar períodos curtos
        periodos_consolidados = []
        
        for p in periodos_brutos:
            duracao = (p['data_fim'] - p['data_inicio']).total_seconds() / 60
            
            if not periodos_consolidados:
                periodos_consolidados.append(p)
                continue
            
            ultimo = periodos_consolidados[-1]
            gap = (p['data_inicio'] - ultimo['data_fim']).total_seconds() / 60
            
            # Regra 1: Se período é muito curto (< 5 min), absorver no anterior
            if duracao < MIN_DURACAO_PERIODO:
                # Estender o período anterior até o fim deste
                ultimo['fim_idx'] = p['fim_idx']
                ultimo['data_fim'] = p['data_fim']
                continue
            
            # Regra 2: Se gap é curto e são do mesmo tipo, consolidar
            if gap <= GAP_CONSOLIDACAO and ultimo['tipo'] == p['tipo']:
                ultimo['fim_idx'] = p['fim_idx']
                ultimo['data_fim'] = p['data_fim']
                continue
            
            # Regra 3: Parada curta entre movimentos (ociosidade em trânsito) - absorver no movimento
            if (ultimo['tipo'] == 'MOVIMENTO' and p['tipo'] == 'PARADA' and 
                duracao < GAP_CONSOLIDACAO):
                # Verificar se o próximo também é movimento
                # Por agora, mantemos como parada curta (será tratado no próximo loop)
                pass
            
            periodos_consolidados.append(p)
        
        # PASSO 3: Segunda passada - consolidar movimentos separados por paradas muito curtas
        periodos_final = []
        i = 0
        while i < len(periodos_consolidados):
            p = periodos_consolidados[i]
            
            if p['tipo'] == 'MOVIMENTO':
                # Verificar se podemos absorver paradas curtas à frente
                while i + 2 < len(periodos_consolidados):
                    parada = periodos_consolidados[i + 1]
                    prox_mov = periodos_consolidados[i + 2]
                    
                    if parada['tipo'] == 'PARADA' and prox_mov['tipo'] == 'MOVIMENTO':
                        duracao_parada = (parada['data_fim'] - parada['data_inicio']).total_seconds() / 60
                        
                        if duracao_parada < GAP_CONSOLIDACAO:
                            # Absorver parada e próximo movimento
                            p['fim_idx'] = prox_mov['fim_idx']
                            p['data_fim'] = prox_mov['data_fim']
                            i += 2
                        else:
                            break
                    else:
                        break
            
            periodos_final.append(p)
            i += 1
        
        # PASSO 4: Construir resultado final com todas as métricas
        # Verificar se o último período ainda está "em curso"
        agora = datetime.now()
        
        # Obter o último raw_id do lote processado para esta placa
        ultimo_raw_id_lote = df_placa['raw_id'].max()
        
        for idx_p, p in enumerate(periodos_final):
            inicio_idx = p['inicio_idx']
            fim_idx = p['fim_idx']
            
            data_fim_periodo = df_placa.loc[fim_idx, 'data_hora']
            raw_id_fim_periodo = df_placa.loc[fim_idx, 'raw_id']
            
            # Verificar se período está "em curso" usando múltiplos critérios:
            # 1. É o último período da placa
            # 2. Terminou há menos de 60 minutos OU raw_id_fim é o último do lote
            is_ultimo = (idx_p == len(periodos_final) - 1)
            tempo_desde_fim = (agora - data_fim_periodo.to_pydatetime().replace(tzinfo=None)).total_seconds() / 60
            is_ultimo_raw_id = (raw_id_fim_periodo == ultimo_raw_id_lote)
            
            # Período em curso se: é o último E (terminou recentemente OU é o último ponto recebido)
            if is_ultimo and (tempo_desde_fim < 60 or is_ultimo_raw_id):
                # Período ainda em curso, pular para próxima execução
                logger.info(f"⏳ Período em curso ignorado: {placa} {p['tipo']} (raw_id_fim={raw_id_fim_periodo}, último={ultimo_raw_id_lote})")
                continue
            
            resultados.append({
                'placa': placa,
                'tipo': 'DESLOCAMENTO' if p['tipo'] == 'MOVIMENTO' else 'PARADA',
                'inicio_idx': inicio_idx,
                'fim_idx': fim_idx,
                'data_inicio': df_placa.loc[inicio_idx, 'data_hora'],
                'data_fim': df_placa.loc[fim_idx, 'data_hora'],
                'odo_inicio': df_placa.loc[inicio_idx, 'odometro'],
                'odo_fim': df_placa.loc[fim_idx, 'odometro'],
                'raw_id_inicio': df_placa.loc[inicio_idx, 'raw_id'],
                'raw_id_fim': df_placa.loc[fim_idx, 'raw_id'],
                'lat_inicio': df_placa.loc[inicio_idx, 'latitude'],
                'lon_inicio': df_placa.loc[inicio_idx, 'longitude'],
                'lat_fim': df_placa.loc[fim_idx, 'latitude'],
                'lon_fim': df_placa.loc[fim_idx, 'longitude'],
            })
    
    return resultados


def classificar_deslocamentos_v4(df):
    """
    Classificador V4 - Lógica baseada em ignição + distância
    
    Regras:
    - DESLOCAMENTO: ignição=1 constante, TERMINA quando ignição=0 por 10+ min
    - PARADA: inicia no primeiro ignição=0, TERMINA quando ignição=1 E dist>=3km
    - OCIOSIDADE: ignição=1 mas parado - NÃO inicia novo deslocamento
    
    Args:
        df: DataFrame com colunas [raw_id, placa, data_hora, ignicao, velocidade, odometro, lat, lon]
    
    Returns:
        Lista de dicionários com os períodos classificados
    """
    resultados = []
    
    # Processar por placa
    for placa in df['placa'].unique():
        df_placa = df[df['placa'] == placa].sort_values('data_hora').reset_index(drop=True)
        
        if df_placa.empty:
            continue
        
        # Calcular diferença de tempo entre posições
        df_placa['time_diff'] = df_placa['data_hora'].diff().dt.total_seconds() / 60
        
        estado = None  # 'DESLOCAMENTO' ou 'PARADA'
        inicio_idx = 0
        odo_inicio_parada = None
        tempo_ign_off_acumulado = 0
        
        for idx, row in df_placa.iterrows():
            ignicao = row['ignicao'] or 0
            velocidade = row['velocidade'] or 0
            time_diff = row['time_diff'] if pd.notna(row['time_diff']) else 0
            odometro = row['odometro'] or 0
            
            if estado is None:
                # Primeiro ponto
                if ignicao == 1:
                    estado = 'DESLOCAMENTO'
                    inicio_idx = idx
                    tempo_ign_off_acumulado = 0
                else:
                    estado = 'PARADA'
                    inicio_idx = idx
                    odo_inicio_parada = odometro
                    
            elif estado == 'DESLOCAMENTO':
                if ignicao == 0:
                    tempo_ign_off_acumulado += time_diff
                    if tempo_ign_off_acumulado >= TEMPO_IGN_OFF_PARADA:
                        # Fecha deslocamento - buscar último ponto com ignição=1
                        fim_deslocamento_idx = idx
                        for back_idx in range(idx, inicio_idx, -1):
                            if df_placa.loc[back_idx, 'ignicao'] == 1:
                                fim_deslocamento_idx = back_idx
                                break
                        
                        resultados.append({
                            'placa': placa,
                            'tipo': 'DESLOCAMENTO',
                            'inicio_idx': inicio_idx,
                            'fim_idx': fim_deslocamento_idx,
                            'data_inicio': df_placa.loc[inicio_idx, 'data_hora'],
                            'data_fim': df_placa.loc[fim_deslocamento_idx, 'data_hora'],
                            'odo_inicio': df_placa.loc[inicio_idx, 'odometro'],
                            'odo_fim': df_placa.loc[fim_deslocamento_idx, 'odometro'],
                            'raw_id_inicio': df_placa.loc[inicio_idx, 'raw_id'],
                            'raw_id_fim': df_placa.loc[fim_deslocamento_idx, 'raw_id'],
                            'lat_inicio': df_placa.loc[inicio_idx, 'latitude'],
                            'lon_inicio': df_placa.loc[inicio_idx, 'longitude'],
                            'lat_fim': df_placa.loc[fim_deslocamento_idx, 'latitude'],
                            'lon_fim': df_placa.loc[fim_deslocamento_idx, 'longitude'],
                        })
                        
                        # Inicia parada
                        estado = 'PARADA'
                        for p_idx in range(fim_deslocamento_idx + 1, idx + 1):
                            if df_placa.loc[p_idx, 'ignicao'] == 0:
                                inicio_idx = p_idx
                                break
                        else:
                            inicio_idx = idx
                        
                        odo_inicio_parada = df_placa.loc[inicio_idx, 'odometro']
                        tempo_ign_off_acumulado = 0
                else:
                    # ignição=1, continua deslocamento
                    tempo_ign_off_acumulado = 0
                        
            elif estado == 'PARADA':
                if ignicao == 1:
                    dist_desde_parada = abs(odometro - odo_inicio_parada) if odo_inicio_parada else 0
                    if dist_desde_parada >= DIST_REINICIO_DESLOCAMENTO:
                        # Fecha parada - buscar último ponto com ignição=0
                        fim_parada_idx = idx
                        for back_idx in range(idx, inicio_idx, -1):
                            if df_placa.loc[back_idx, 'ignicao'] == 0:
                                fim_parada_idx = back_idx
                                break
                        
                        resultados.append({
                            'placa': placa,
                            'tipo': 'PARADA',
                            'inicio_idx': inicio_idx,
                            'fim_idx': fim_parada_idx,
                            'data_inicio': df_placa.loc[inicio_idx, 'data_hora'],
                            'data_fim': df_placa.loc[fim_parada_idx, 'data_hora'],
                            'odo_inicio': df_placa.loc[inicio_idx, 'odometro'],
                            'odo_fim': df_placa.loc[fim_parada_idx, 'odometro'],
                            'raw_id_inicio': df_placa.loc[inicio_idx, 'raw_id'],
                            'raw_id_fim': df_placa.loc[fim_parada_idx, 'raw_id'],
                            'lat_inicio': df_placa.loc[inicio_idx, 'latitude'],
                            'lon_inicio': df_placa.loc[inicio_idx, 'longitude'],
                            'lat_fim': df_placa.loc[fim_parada_idx, 'latitude'],
                            'lon_fim': df_placa.loc[fim_parada_idx, 'longitude'],
                        })
                        
                        # Inicia novo deslocamento
                        estado = 'DESLOCAMENTO'
                        for d_idx in range(fim_parada_idx + 1, idx + 1):
                            if df_placa.loc[d_idx, 'ignicao'] == 1:
                                inicio_idx = d_idx
                                break
                        else:
                            inicio_idx = idx
                        
                        tempo_ign_off_acumulado = 0
                    # Se dist < 3km, continua na parada (ociosidade)
        
        # Fechar último período
        if estado is not None and len(df_placa) > 0:
            last_idx = len(df_placa) - 1
            resultados.append({
                'placa': placa,
                'tipo': estado,
                'inicio_idx': inicio_idx,
                'fim_idx': last_idx,
                'data_inicio': df_placa.loc[inicio_idx, 'data_hora'],
                'data_fim': df_placa.iloc[-1]['data_hora'],
                'odo_inicio': df_placa.loc[inicio_idx, 'odometro'],
                'odo_fim': df_placa.iloc[-1]['odometro'],
                'raw_id_inicio': df_placa.loc[inicio_idx, 'raw_id'],
                'raw_id_fim': df_placa.iloc[-1]['raw_id'],
                'lat_inicio': df_placa.loc[inicio_idx, 'latitude'],
                'lon_inicio': df_placa.loc[inicio_idx, 'longitude'],
                'lat_fim': df_placa.iloc[-1]['latitude'],
                'lon_fim': df_placa.iloc[-1]['longitude'],
            })
    
    return resultados

    conn.close()
    return ultimo_id


def obter_ultimo_estado_banco(placa):
    """
    V6: Busca o último registro de deslocamento/parada do veículo no banco.
    Isso permite continuar o estado anterior (evitando gaps) em vez de começar do zero.
    """
    conn = get_connection()
    c = conn.cursor()
    
    # Busca o último registro (seja CONCLUIDO ou EM_CURSO)
    query = """
    SELECT id, tipo_parada, data_inicio, data_fim, km_inicial, km_final, 
           distancia, local_inicio, local_fim, raw_id_fim
    FROM deslocamentos 
    WHERE placa = %s 
    ORDER BY data_fim DESC, id DESC 
    LIMIT 1
    """
    
    try:
        c.execute(query, (placa,))
        row = c.fetchone()
        
        if row:
            # Converter para dicionário de estado
            return {
                'id': row[0],
                'tipo': 'MOVIMENTO' if row[1] == 'MOVIMENTO' else 'PARADA', # Normalizar nome
                'data_fim': pd.to_datetime(row[3]),
                'km_final': row[5],
                'local_fim': row[8],
                'raw_id_fim': row[9]
            }
    except Exception as e:
        logger.error(f"Erro ao buscar último estado para {placa}: {e}")
        
    conn.close()
    return None

def classificar_deslocamentos_v6_persistente(df, estado_atual=None):
    """
    Classificador V6 - Máquina de Estados Persistente
    
    Diferente do V5, este classificador leva em conta o último estado gravado no banco.
    
    Cenários:
    1. Sem estado anterior: Inicia lógica V5 normal
    2. Anterior = MOVIMENTO:
       - Novos dados continuam movendo? -> UPDATE registro anterior (estende data_fim)
       - Parou? -> UPDATE registro anterior (fecha) + INSERT nova Parada (inicia onde mov terminou)
       
    3. Anterior = PARADA:
       - Novos dados continuam parados? -> UPDATE registro anterior
       - Moveu? -> UPDATE registro anterior (fecha) + INSERT novo Movimento
       
    Returns:
        updates: Lista de atualizações em registros existentes
        inserts: Lista de novos registros a criar
        novo_estado: O estado final após processar o lote
    """
    updates = []
    inserts = []
    
    # Se não temos dados novos, retorna nada
    if df.empty:
        return updates, inserts, estado_atual

    # Ordenar por garantia
    df = df.sort_values('data_hora').reset_index(drop=True)
    
    # Calcular métricas básicas para análise (velocidade, tempo, dist)
    df['time_diff'] = df['data_hora'].diff().dt.total_seconds() / 60
    
    # Lógica de Estado
    # Se não tem estado anterior, assumimos o primeiro ponto como início de algo
    if estado_atual is None:
        # Tenta inferir pelo primeiro ponto
        primeiro = df.iloc[0]
        vel = primeiro['velocidade']
        tipo_inicial = 'MOVIMENTO' if vel >= VELOCIDADE_MOVIMENTO else 'PARADA'
        
        # Cria um estado virtual "novo" para começar a processar
        estado_atual = {
            'id': None, # None indica que é um registro novo, ainda não salvo
            'tipo': tipo_inicial,
            'inicio_idx': 0,
            'data_inicio': primeiro['data_hora'],
            'data_fim': primeiro['data_hora'],
            'raw_id_fim': primeiro['raw_id']
        }
    
    # Processar ponto a ponto (ou em blocos para performance)
    # Por simplicidade e robustez V6, vamos iterar e detectar transições
    
    # Processar ponto a ponto para detectar transições
    pontos = df.to_dict('records')
    
    # Se estado_atual veio do banco, precisamos carregar seu 'contexto' (ID, tipo)
    current_state_type = estado_atual['tipo']
    current_db_id = estado_atual.get('id')
    current_start_idx = 0  # Índice relativo ao DF atual onde começou este segmento
    
    # Buffer para confirmar transição
    transition_buffer = []  # Lista de pontos candidatos a mudança
    
    for i, p in enumerate(pontos):
        # Determinar estado instantâneo deste ponto
        is_moving = p['velocidade'] >= VELOCIDADE_MOVIMENTO
        point_type = 'MOVIMENTO' if is_moving else 'PARADA'
        
        # Lógica de Confirmação de Transição
        if point_type != current_state_type:
            # Potencial mudança de estado! Adicionar ao buffer
            transition_buffer.append(p)
            
            # Verificar se confirmamos a mudança
            start_transition = transition_buffer[0]['data_hora']
            end_transition = p['data_hora']
            duration_minutes = (end_transition - start_transition).total_seconds() / 60
            
            confirm_threshold = GAP_CONSOLIDACAO if current_state_type == 'MOVIMENTO' else MIN_DURACAO_PERIODO
            
            if duration_minutes >= confirm_threshold:
                # MUDANÇA CONFIRMADA!
                # A mudança aconteceu em transition_buffer[0] (retroativo)
                transition_start_point = transition_buffer[0]
                
                # 1. Fechar estado anterior (current)
                # Se tem ID, é UPDATE. Se não, é INSERT de um novo fechado.
                prev_end_point = pontos[i - len(transition_buffer)] # Ponto antes da transição começar
                
                # Se não houver ponto anterior no DF (mudança logo no início), usar o próprio início da transição
                # como fim do anterior (continuidade perfeita)
                data_fim_anterior = transition_start_point['data_hora']
                
                if current_db_id:
                    # UPDATE registro existente no banco
                    updates.append({
                        'id': current_db_id,
                        'data_fim': data_fim_anterior,
                        'km_final': transition_start_point['odometro'], # Aproximado
                        'local_fim': f"{transition_start_point['latitude']}, {transition_start_point['longitude']}", # Será geocodificado depois
                        'raw_id_fim': transition_start_point['raw_id']
                    })
                else:
                    # CORREÇÃO V6: Fechar o INSERT anterior que ainda estava aberto
                    # Isso acontece quando há múltiplas transições no mesmo lote
                    if inserts:
                        # Preencher data_fim do último insert com o momento da transição
                        inserts[-1]['data_fim'] = data_fim_anterior
                        inserts[-1]['km_final'] = transition_start_point['odometro']
                        inserts[-1]['local_fim'] = f"{transition_start_point['latitude']}, {transition_start_point['longitude']}"
                        inserts[-1]['raw_id_fim'] = transition_start_point['raw_id']
                
                # 2. Iniciar novo estado
                current_state_type = point_type
                current_db_id = None # Novo estado, ainda não tem ID no banco
                current_start_idx = i - len(transition_buffer) + 1 # Ajustar índice (aprox)
                transition_buffer = [] # Limpar buffer
                
                # Adicionar novo registro para INSERT
                inserts.append({
                    'tipo': current_state_type,
                    'data_inicio': data_fim_anterior, # Início = Fim do anterior (GAP ZERO)
                    'km_inicial': transition_start_point['odometro'],
                    'local_inicio': f"{transition_start_point['latitude']}, {transition_start_point['longitude']}",
                    'raw_id_inicio': transition_start_point['raw_id'],
                    # Fim será definido quando fechar ou no final do lote
                })
                
        else:
            # Ponto confirma o estado atual (resetar buffer de transição)
            # Ex: Estava MOVIMENTO, veio PARADA, PARADA (buffer), depois MOVIMENTO (reset)
            if transition_buffer:
                transition_buffer = []
        
    # Fim do loop: O que sobrou é o estado atual "EM CURSO"
    # Precisamos atualizar o registro correspondente (seja UPDATE no banco ou UPDATE no INSERT pendente)
    
    ultimo_ponto = pontos[-1]
    
    if current_db_id:
        # O estado continuou o mesmo do banco o tempo todo (ou desde a última transição confirmada)
        updates.append({
            'id': current_db_id,
            'data_fim': ultimo_ponto['data_hora'],
            'km_final': ultimo_ponto['odometro'],
            'local_fim': f"{ultimo_ponto['latitude']}, {ultimo_ponto['longitude']}",
            'raw_id_fim': ultimo_ponto['raw_id']
        })
    elif inserts:
        # Temos um insert novo aberto, vamos fechar ele no fim deste lote
        inserts[-1]['data_fim'] = ultimo_ponto['data_hora']
        inserts[-1]['km_final'] = ultimo_ponto['odometro']
        inserts[-1]['local_fim'] = f"{ultimo_ponto['latitude']}, {ultimo_ponto['longitude']}"
        inserts[-1]['raw_id_fim'] = ultimo_ponto['raw_id']
    else:
        # Caso raro: Estado novo desde o início mas sem transição anterior
        # (Ex: Primeiro dado da vida do caminhão)
        inserts.append({
            'tipo': current_state_type,
            'data_inicio': pontos[0]['data_hora'],
            'km_inicial': pontos[0]['odometro'],
            'local_inicio': f"{pontos[0]['latitude']}, {pontos[0]['longitude']}",
            'raw_id_inicio': pontos[0]['raw_id'],
            'data_fim': ultimo_ponto['data_hora'],
            'km_final': ultimo_ponto['odometro'],
            'local_fim': f"{ultimo_ponto['latitude']}, {ultimo_ponto['longitude']}",
            'raw_id_fim': ultimo_ponto['raw_id']
        })

    # Retornar o novo estado para o próximo lote (se precisarmos em memória)
    novo_estado = {
        'tipo': current_state_type,
        'id': current_db_id or 'PENDING_INSERT'
    }

    return updates, inserts, novo_estado



def obter_ultimo_raw_id_processado():
    """
    Busca o maior raw_id_fim já processado.
    Permite processamento incremental sem reprocessar dados antigos.
    """
    conn = get_connection()
    c = conn.cursor()
    
    try:
        c.execute("SELECT MAX(raw_id_fim) FROM deslocamentos WHERE raw_id_fim IS NOT NULL")
        result = c.fetchone()
        ultimo_id = result[0] if result and result[0] else 0
    except Exception as e:
        # Coluna pode não existir em bancos antigos
        # print(f"⚠️ Erro ao buscar último ID processado: {e}")
        ultimo_id = 0
    
    conn.close()
    return ultimo_id

def processar_deslocamentos(reprocessar_tudo=False):
    """
    Processador V6 - Estado Persistente
    
    Lógica baseada em Máquina de Estados que persiste no banco para eliminar gaps.
    Sempre carrega o último estado do banco e continua a partir dele.
    """
    print("🚀 Iniciando Processador V6 (Estado Persistente)...")
    
    # Garantir que as novas colunas existam
    try:
        migrate_db()
    except Exception as e:
        print(f"⚠️ Migração: {e}")
    
    conn = get_connection()
    c = conn.cursor()
    
    # Determinar ponto de início
    if reprocessar_tudo:
        ultimo_id = 0
        print("⚠️ Modo REPROCESSAR TUDO ativado")
    else:
        ultimo_id = obter_ultimo_raw_id_processado()
        print(f"📍 Último raw_id processado: {ultimo_id}")
    
    # 1. Carregar apenas dados NOVOS (incremental)
    query = f"""
        SELECT 
            p.id AS raw_id,
            p.id_veiculo, 
            v.placa, 
            p.data_hora, 
            p.latitude, 
            p.longitude, 
            p.odometro, 
            p.ignicao, 
            p.velocidade
        FROM posicoes_raw p
        JOIN veiculos v ON p.id_veiculo = v.id_sascar
        WHERE p.id > %s
        ORDER BY p.id_veiculo, p.data_hora
    """
    
    try:
        df = pd.read_sql(query, conn, params=(ultimo_id,))
    except Exception as e:
        print(f"Erro ao ler dados: {e}")
        conn.close()
        return []
    
    if df.empty:
        print("✅ Nenhum dado novo para processar.")
        conn.close()
        return []
    
    print(f"📦 Dados novos carregados: {len(df)} linhas")
    
    # Converter data
    df['data_hora'] = pd.to_datetime(df['data_hora'])
    
    print("🔄 Classificando com Lógica V6 (Máquina de Estados Persistente)...")
    
    total_updates = 0
    total_inserts = 0
    placas_processadas = 0
    
    for placa in df['placa'].unique():
        df_placa = df[df['placa'] == placa].copy()
        placas_processadas += 1
        
        # Carregar último estado do banco
        estado_banco = obter_ultimo_estado_banco(placa)
        
        # Rodar classificador V6
        updates, inserts, novo_estado = classificar_deslocamentos_v6_persistente(df_placa, estado_banco)
        
        # Executar UPDATES
        if updates:
            total_updates += len(updates)
            for upd in updates:
                # Geocodificar local_fim se for coordenada
                local_fim_raw = upd.get('local_fim', '')
                try:
                    parts = local_fim_raw.split(',')
                    if len(parts) == 2:
                        lat, lon = float(parts[0].strip()), float(parts[1].strip())
                        local_fim = get_cached_city_name(lat, lon)
                    else:
                        local_fim = local_fim_raw
                except:
                    local_fim = local_fim_raw
                    
                c.execute("""
                    UPDATE deslocamentos 
                    SET data_fim = %s, km_final = %s, local_fim = %s, raw_id_fim = %s,
                        tempo = EXTRACT(EPOCH FROM (%s - data_inicio))/60
                    WHERE id = %s
                """, (
                    upd['data_fim'], upd['km_final'], local_fim, upd['raw_id_fim'],
                    upd['data_fim'], upd['id']
                ))
        
        # Executar INSERTS
        if inserts:
            total_inserts += len(inserts)
            for ins in inserts:
                dist = (ins.get('km_final', 0) - ins['km_inicial']) if 'km_final' in ins else 0
                
                # Inicia como 'PENDENTE' para aparecer na interface de fechamento
                # Podemos também usar 'EM_CURSO' se ainda não fechou, mas interface filtra PENDENTE
                status = 'PENDENTE'
                
                # CORREÇÃO V6: Se o registro é EM_CURSO, ele pode não ter data_fim definida ainda no dicionário ins.
                # Mas o banco exige NOT NULL. Então usamos a última data conhecida como data_fim provisória.
                # A lógica V6 vai estender essa data via UPDATE nas próximas execuções.
                
                dt_fim = ins.get('data_fim', ins['data_inicio']) # Se não tem fim, fim = inicio (duração 0)
                km_final = ins.get('km_final', ins['km_inicial'])
                raw_id_fim = ins.get('raw_id_fim', ins['raw_id_inicio'])
                
                # GEOCODIFICAÇÃO DIRETA: Converter lat/long para nomes de cidades
                # O local_inicio e local_fim vêm como "lat, lon" do classificador
                local_inicio_raw = ins.get('local_inicio', '')
                local_fim_raw = ins.get('local_fim', ins.get('local_inicio', ''))
                
                # Extrair lat/lon e geocodificar
                try:
                    parts_ini = local_inicio_raw.split(',')
                    if len(parts_ini) == 2:
                        lat_i, lon_i = float(parts_ini[0].strip()), float(parts_ini[1].strip())
                        local_inicio = get_cached_city_name(lat_i, lon_i)
                    else:
                        local_inicio = local_inicio_raw
                except:
                    local_inicio = local_inicio_raw
                
                try:
                    parts_fim = local_fim_raw.split(',')
                    if len(parts_fim) == 2:
                        lat_f, lon_f = float(parts_fim[0].strip()), float(parts_fim[1].strip())
                        local_fim = get_cached_city_name(lat_f, lon_f)
                    else:
                        local_fim = local_fim_raw
                except:
                    local_fim = local_fim_raw
                
                c.execute("""
                    INSERT INTO deslocamentos (
                        placa, tipo_parada, data_inicio, data_fim, 
                        km_inicial, km_final, distancia, 
                        local_inicio, local_fim, 
                        raw_id_inicio, raw_id_fim,
                        status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    placa, ins['tipo'], ins['data_inicio'], dt_fim,
                    ins['km_inicial'], km_final, dist,
                    local_inicio, local_fim,
                    ins['raw_id_inicio'], raw_id_fim,
                    status
                ))
        
        conn.commit()
    
    print("\n" + "="*50)
    print("📊 RESUMO DO PROCESSAMENTO V6")
    print("="*50)
    print(f"  Placas processadas: {placas_processadas}")
    print(f"  Registros atualizados: {total_updates}")
    print(f"  Novos registros: {total_inserts}")
    
    conn.close()
    
    # 3. Geo-decodificação assíncrona (Backfill)
    try:
        from backfill_names import processar_backfill
        print("🌍 Iniciando backfill de geocodificação...")
        processar_backfill()
    except Exception as e:
        print(f"⚠️ Erro no backfill: {e}")
        
    return [] # Compatibilidade + 1


def consolidar_periodos_consecutivos(tolerancia_minutos=30):
    """
    Consolida paradas e movimentos consecutivos do mesmo veículo no mesmo local.
    
    Esta função agrupa registros fragmentados que deveriam ser um único período.
    Por exemplo: vários registros de PARADA de 20-30 minutos consecutivos
    são consolidados em um único registro de PARADA de várias horas.
    
    Args:
        tolerancia_minutos: Gap máximo entre períodos para considerar como consecutivos (default: 30)
    
    Regras de consolidação:
    - PARADAS consecutivas: mesmo local_inicio, gap < tolerância
    - MOVIMENTOS consecutivos: local_fim do anterior = local_inicio do próximo, gap < tolerância
    """
    print(f"🔄 Iniciando consolidação de períodos consecutivos (tolerância: {tolerancia_minutos} min)...")
    
    conn = get_connection()
    c = conn.cursor()
    
    # Buscar deslocamentos pendentes ordenados por placa e data
    c.execute("""
        SELECT id, placa, tipo_parada, data_inicio, data_fim, 
               km_inicial, km_final, distancia, local_inicio, local_fim,
               tempo, tempo_ocioso, tempo_motor_off, qtd_pontos,
               raw_id_inicio, raw_id_fim
        FROM deslocamentos 
        WHERE status = 'PENDENTE'
        ORDER BY placa, data_inicio
    """)
    
    registros = c.fetchall()
    
    if not registros:
        print("ℹ️ Nenhum deslocamento pendente para consolidar.")
        conn.close()
        return
    
    print(f"📦 {len(registros)} registros pendentes encontrados.")
    
    # Agrupar por placa
    registros_por_placa = {}
    for reg in registros:
        placa = reg[1]
        if placa not in registros_por_placa:
            registros_por_placa[placa] = []
        registros_por_placa[placa].append({
            'id': reg[0],
            'placa': reg[1],
            'tipo': reg[2],
            'data_inicio': pd.to_datetime(reg[3]),
            'data_fim': pd.to_datetime(reg[4]),
            'km_inicial': reg[5] or 0,
            'km_final': reg[6] or 0,
            'distancia': reg[7] or 0,
            'local_inicio': reg[8],
            'local_fim': reg[9],
            'tempo': reg[10] or 0,
            'tempo_ocioso': reg[11] or 0,
            'tempo_motor_off': reg[12] or 0,
            'qtd_pontos': reg[13] or 0,
            'raw_id_inicio': reg[14],
            'raw_id_fim': reg[15],
        })
    
    ids_para_deletar = []
    registros_para_atualizar = []
    total_consolidados = 0
    
    for placa, lista_reg in registros_por_placa.items():
        if len(lista_reg) < 2:
            continue
        
        i = 0
        while i < len(lista_reg):
            reg_atual = lista_reg[i]
            grupo = [reg_atual]
            
            # Buscar consecutivos que podem ser consolidados
            j = i + 1
            while j < len(lista_reg):
                reg_prox = lista_reg[j]
                
                # Calcular gap entre fim do atual e início do próximo
                gap = (reg_prox['data_inicio'] - grupo[-1]['data_fim']).total_seconds() / 60
                
                # Verificar se pode consolidar
                pode_consolidar = False
                
                if gap <= tolerancia_minutos:
                    # Mesmo tipo (PARADA com PARADA, MOVIMENTO com MOVIMENTO)
                    if reg_atual['tipo'] == reg_prox['tipo']:
                        # V5: Sempre consolida se mesmo tipo e gap pequeno
                        # Ignora comparação de local (coordenadas GPS variam levemente)
                        pode_consolidar = True
                
                if pode_consolidar:
                    grupo.append(reg_prox)
                    j += 1
                else:
                    break
            
            # Se temos mais de 1 registro no grupo, consolidar
            if len(grupo) > 1:
                primeiro = grupo[0]
                ultimo = grupo[-1]
                
                # Calcular métricas agregadas
                tempo_total = (ultimo['data_fim'] - primeiro['data_inicio']).total_seconds() / 60
                distancia_total = sum(r['distancia'] for r in grupo)
                tempo_ocioso_total = sum(r['tempo_ocioso'] for r in grupo)
                tempo_motor_off_total = sum(r['tempo_motor_off'] for r in grupo)
                qtd_pontos_total = sum(r['qtd_pontos'] for r in grupo)
                
                # Atualizar o primeiro registro com dados consolidados
                registros_para_atualizar.append({
                    'id': primeiro['id'],
                    'data_fim': ultimo['data_fim'].strftime('%Y-%m-%d %H:%M:%S'),
                    'km_final': ultimo['km_final'],
                    'distancia': distancia_total,
                    'local_fim': ultimo['local_fim'],
                    'tempo': tempo_total,
                    'tempo_ocioso': tempo_ocioso_total,
                    'tempo_motor_off': tempo_motor_off_total,
                    'qtd_pontos': qtd_pontos_total,
                    'raw_id_fim': ultimo['raw_id_fim'],
                })
                
                # Marcar os demais para deleção
                for r in grupo[1:]:
                    ids_para_deletar.append(r['id'])
                
                total_consolidados += len(grupo) - 1
            
            i = j
    
    # Executar atualizações
    if registros_para_atualizar:
        print(f"📝 Atualizando {len(registros_para_atualizar)} registros consolidados...")
        for reg in registros_para_atualizar:
            c.execute("""
                UPDATE deslocamentos 
                SET data_fim = %s, km_final = %s, distancia = %s, local_fim = %s,
                    tempo = %s, tempo_ocioso = %s, tempo_motor_off = %s, 
                    qtd_pontos = %s, raw_id_fim = %s
                WHERE id = %s
            """, (
                reg['data_fim'], reg['km_final'], reg['distancia'], reg['local_fim'],
                reg['tempo'], reg['tempo_ocioso'], reg['tempo_motor_off'],
                reg['qtd_pontos'], reg['raw_id_fim'], reg['id']
            ))
        conn.commit()
    
    # Deletar registros consolidados
    if ids_para_deletar:
        print(f"🗑️ Removendo {len(ids_para_deletar)} registros duplicados após consolidação...")
        # Deletar em lotes para evitar query muito grande
        BATCH_SIZE = 100
        for i in range(0, len(ids_para_deletar), BATCH_SIZE):
            batch_ids = ids_para_deletar[i:i + BATCH_SIZE]
            placeholders = ', '.join(['%s'] * len(batch_ids))
            c.execute(f"DELETE FROM deslocamentos WHERE id IN ({placeholders})", batch_ids)
        conn.commit()
    
    conn.close()
    
    print(f"\n✅ Consolidação concluída!")
    print(f"   - Registros consolidados: {total_consolidados}")
    print(f"   - Registros removidos: {len(ids_para_deletar)}")
    print(f"   - Registros atualizados: {len(registros_para_atualizar)}")


def limpar_e_reprocessar():
    """
    Limpa TODOS os deslocamentos e reprocessa do zero.
    USE COM CUIDADO - apenas quando necessário reconstruir tudo.
    """
    print("⚠️ ATENÇÃO: Limpando TODOS os deslocamentos para reprocessamento completo...")
    
    conn = get_connection()
    c = conn.cursor()
    
    # Contar antes
    c.execute("SELECT COUNT(*) FROM deslocamentos")
    qtd_antes = c.fetchone()[0]
    
    # Deletar TODOS os deslocamentos (reprocessar = reconstruir do zero)
    c.execute("DELETE FROM deslocamentos")
    conn.commit()
    conn.close()
    
    print(f"🗑️ {qtd_antes} deslocamentos removidos.")
    print("🔄 Iniciando reprocessamento completo...")
    
    # Reprocessar tudo
    processar_deslocamentos(reprocessar_tudo=True)


def remover_duplicatas():
    """
    Remove deslocamentos e posições duplicadas do banco de dados.
    Duplicatas são identificadas por placa + data_inicio + data_fim iguais.
    """
    print("🔍 Buscando duplicatas...")
    
    conn = get_connection()
    c = conn.cursor()
    
    # 1. Remover deslocamentos duplicados (mantém o de menor ID)
    print("📋 Removendo deslocamentos duplicados...")
    
    # Identificar duplicatas (PostgreSQL syntax)
    duplicatas_query = """
        DELETE FROM deslocamentos 
        WHERE id NOT IN (
            SELECT MIN(id) 
            FROM deslocamentos 
            GROUP BY placa, data_inicio, data_fim
        )
    """
    
    try:
        c.execute(duplicatas_query)
        qtd_desloc = c.rowcount
        print(f"  ✅ {qtd_desloc} deslocamentos duplicados removidos")
    except Exception as e:
        print(f"  ⚠️ Erro ao remover duplicatas de deslocamentos: {e}")
        qtd_desloc = 0
    
    # 2. Remover posições duplicadas (mantém a de menor ID)
    print("📍 Removendo posições duplicadas...")
    
    posicoes_query = """
        DELETE FROM posicoes_raw 
        WHERE id NOT IN (
            SELECT MIN(id) 
            FROM posicoes_raw 
            GROUP BY id_veiculo, data_hora
        )
    """
    
    try:
        c.execute(posicoes_query)
        qtd_pos = c.rowcount
        print(f"  ✅ {qtd_pos} posições duplicadas removidas")
    except Exception as e:
        print(f"  ⚠️ Erro ao remover duplicatas de posições: {e}")
        qtd_pos = 0
    
    conn.commit()
    conn.close()
    
    print(f"\n📊 Total removido: {qtd_desloc} deslocamentos + {qtd_pos} posições")


def corrigir_nomes_locais():
    """
    Corrige nomes de locais existentes no banco de dados.
    Remove prefixos verbosos como 'Região Geográfica Imediata de'.
    """
    print("📝 Corrigindo nomes de locais existentes...")
    
    conn = get_connection()
    c = conn.cursor()
    
    # Substituições a fazer
    substituicoes = [
        ('Região Geográfica Imediata de ', 'Região de '),
        ('Região Geográfica Intermediária de ', 'Região de '),
        ('Microrregião de ', ''),
        ('Mesorregião de ', ''),
    ]
    
    total_corrigidos = 0
    
    for antigo, novo in substituicoes:
        # Corrigir local_inicio
        c.execute(f"""
            UPDATE deslocamentos 
            SET local_inicio = REPLACE(local_inicio, %s, %s)
            WHERE local_inicio LIKE %s
        """, (antigo, novo, f'%{antigo}%'))
        total_corrigidos += c.rowcount
        
        # Corrigir local_fim
        c.execute(f"""
            UPDATE deslocamentos 
            SET local_fim = REPLACE(local_fim, %s, %s)
            WHERE local_fim LIKE %s
        """, (antigo, novo, f'%{antigo}%'))
        total_corrigidos += c.rowcount
    
    conn.commit()
    conn.close()
    
    print(f"✅ {total_corrigidos} campos de local corrigidos")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        comando = sys.argv[1]
        
        if comando == "--reprocessar":
            limpar_e_reprocessar()
            # V6: Não precisa consolidar, a lógica já elimina gaps
        elif comando == "--limpar-duplicatas":
            remover_duplicatas()
        elif comando == "--corrigir-nomes":
            corrigir_nomes_locais()
        elif comando == "--consolidar":
            # Opção para consolidar manualmente
            tolerancia = 30  # default
            if len(sys.argv) > 2:
                try:
                    tolerancia = int(sys.argv[2])
                except ValueError:
                    print(f"⚠️ Tolerância inválida: {sys.argv[2]}. Usando 30 minutos.")
            consolidar_periodos_consecutivos(tolerancia)
        elif comando == "--help":
            print("""
Processador de Deslocamentos v5
================================
Uso: python processor.py [opção]

Opções:
  (sem opção)           Processamento incremental normal + consolidação
  --reprocessar         Limpa pendentes e reprocessa tudo + consolida
  --consolidar [min]    Consolida paradas/movimentos fragmentados (default: 30 min)
  --limpar-duplicatas   Remove deslocamentos e posições duplicados
  --corrigir-nomes      Corrige nomes de locais verbosos
  --help                Mostra esta ajuda

Lógica V5:
  - MOVIMENTO: velocidade >= 3 km/h
  - PARADA: velocidade < 3 km/h
  - Períodos < 5 min são absorvidos no anterior
  - Paradas < 15 min entre movimentos não fragmentam
  - Períodos "em curso" (< 30 min) não são inseridos
            """)
        else:
            print(f"Opção desconhecida: {comando}")
            print("Use --help para ver as opções disponíveis")
    else:
        # Processamento normal + consolidação automática
        processar_deslocamentos()
        consolidar_periodos_consecutivos()

