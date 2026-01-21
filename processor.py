"""
Processador de Deslocamentos v3
================================
Versão melhorada com:
- Processamento INCREMENTAL (sem DELETE destrutivo)
- Cálculo de tempo ocioso real
- Detecção de perda de sinal vs parada intencional
- Rastreabilidade com raw_id_inicio e raw_id_fim
"""

import pandas as pd
import numpy as np
from datetime import datetime
from functools import lru_cache
from database import get_connection, get_placeholder, get_pois, migrate_db

# Usar apenas a base
from poi_data import POIS_NUPORANGA

# ==========================================
# CONFIGURAÇÕES
# ==========================================
GAP_THRESHOLD_MINUTES = 20      # Tempo de gap para considerar nova viagem
STOP_THRESHOLD_KMH = 3          # Velocidade abaixo = parado
MIN_DISTANCIA_VIAGEM = 2     # Viagens < 0.5km são ruído/manobra
SIGNAL_LOSS_THRESHOLD = 60      # > 60min sem sinal com ignição = provável perda de sinal
MAX_SPEED_REALISTIC = 150       # Velocidade máxima realista (km/h) - acima é erro de GPS

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


@lru_cache(maxsize=2000)
def get_cached_city_name(lat, lon):
    """
    Geocodificação reversa com cache e arredondamento para maximizar hits.
    Arredonda para 3 casas (aprox 100m) para agrupar locais próximos.
    """
    lat_r = round(lat, 3)
    lon_r = round(lon, 3)
    
    # 1. Tentar Base Nuporanga (POIs locais)
    for name, coords_list in POIS_NUPORANGA.items():
        for (p_lat, p_lon) in coords_list:
            if abs(lat_r - p_lat) < 0.01 and abs(lon_r - p_lon) < 0.01:
                return name

    # 2. OpenStreetMap / Nominatim
    if not GEOPY_AVAILABLE:
        return "Local Desconhecido"
        
    try:
        geolocator = Nominatim(user_agent="frota_cf_v3", timeout=3)
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
        print(f"⚠️ Erro ao buscar último ID processado: {e}")
        ultimo_id = 0
    
    conn.close()
    return ultimo_id


def processar_deslocamentos(reprocessar_tudo=False):
    """
    Processador V3 - Incremental e Preciso
    
    Args:
        reprocessar_tudo: Se True, ignora processamento incremental e reprocessa tudo.
                         CUIDADO: Isso pode criar duplicatas se não limpar antes!
    """
    print("🚀 Iniciando Processador V3 (Incremental)...")
    
    # Garantir que as novas colunas existam
    try:
        migrate_db()
    except Exception as e:
        print(f"⚠️ Migração: {e}")
    
    conn = get_connection()
    
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
        WHERE p.id > {get_placeholder(1)}
        ORDER BY p.id_veiculo, p.data_hora
    """
    
    df = pd.read_sql(query, conn, params=(ultimo_id,))
    
    if df.empty:
        print("✅ Nenhum dado novo para processar.")
        conn.close()
        return
    
    print(f"📦 Dados novos carregados: {len(df)} linhas")
    
    # Converter data
    df['data_hora'] = pd.to_datetime(df['data_hora'])
    
    # 2. Calcular diferenças de tempo entre posições consecutivas
    df['time_diff'] = df.groupby('placa')['data_hora'].diff().dt.total_seconds() / 60
    
    # Calcular velocidade média dos últimos N pontos (para classificar gaps)
    df['velocidade_media'] = df.groupby('placa')['velocidade'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean().shift(1)
    )
    
    # ========================================================================
    # LÓGICA CORRETA DE DESLOCAMENTO:
    # - Deslocamento CONTINUA enquanto ignição está LIGADA (mesmo parado)
    # - Deslocamento SÓ TERMINA quando TEMPO ACUMULADO com ignição OFF >= 10 min
    # - Gap de sinal > 20 min também finaliza (pode ter desligado)
    # ========================================================================
    
    TEMPO_MIN_PARADA_MINUTOS = 10  # Tempo mínimo ACUMULADO com ignição 0 para finalizar viagem
    
    # Calcular TEMPO ACUMULADO com ignição desligada por veículo
    # Se ignição=0, acumula o time_diff. Se ignição=1, reseta para 0.
    def calcular_tempo_acumulado_off(group):
        tempo_off_acumulado = []
        acumulado = 0
        
        for idx, row in group.iterrows():
            if row['ignicao'] == 0:
                # Ignição desligada - acumula o tempo
                acumulado += row['time_diff'] if pd.notna(row['time_diff']) else 0
            else:
                # Ignição ligada - reseta mas primeiro guarda o valor acumulado
                pass
            
            tempo_off_acumulado.append(acumulado)
            
            # Se ignição ligou, reseta o acumulador DEPOIS de guardar
            if row['ignicao'] == 1:
                acumulado = 0
        
        return pd.Series(tempo_off_acumulado, index=group.index)
    
    df['tempo_off_acumulado'] = df.groupby('placa', group_keys=False).apply(calcular_tempo_acumulado_off)
    
    df['last_ignicao'] = df.groupby('placa')['ignicao'].shift(1)
    df['last_tempo_off'] = df.groupby('placa')['tempo_off_acumulado'].shift(1)
    
    # Identificar quando ignição LIGA (0→1) após período desligado
    df['ignicao_ligou'] = (df['last_ignicao'] == 0) & (df['ignicao'] == 1)
    
    # Um NOVO DESLOCAMENTO começa quando:
    # 1. Primeiro registro do veículo (sem dados anteriores)
    # 2. Ignição LIGA E tempo acumulado com ignição OFF foi >= 10 min
    # 3. Gap de sinal > 20 min (assumimos que desligou e religou)
    
    df['parada_prolongada'] = (
        (df['ignicao_ligou']) &           # Ignição acabou de ligar
        (df['last_tempo_off'] >= TEMPO_MIN_PARADA_MINUTOS)  # Ficou desligada 10+ min ACUMULADOS
    )
    
    # Também nova viagem se gap muito longo (perda de sinal)
    df['gap_longo'] = df['time_diff'] > GAP_THRESHOLD_MINUTES
    
    # Uma nova viagem começa quando:
    df['new_trip'] = (
        df['time_diff'].isna() |      # Primeiro registro do veículo
        df['parada_prolongada'] |      # Ignição ficou desligada 10+ min ACUMULADOS e religou
        df['gap_longo']                # Gap de sinal > 20 min
    )
    
    # Classificar tipo de parada (para exibição)
    df['tipo_gap'] = df.apply(
        lambda row: classificar_tipo_parada(
            row['time_diff'] if pd.notna(row['time_diff']) else 0,
            row['last_ignicao'] if pd.notna(row['last_ignicao']) else 1,
            row['velocidade_media']
        ),
        axis=1
    )
    
    # Criar ID único de viagem
    df['trip_id'] = df['new_trip'].cumsum()
    
    viagens_brutas = df['trip_id'].max()
    print(f"📊 Viagens brutas identificadas: {viagens_brutas}")
    
    # 3. Agregar dados por viagem
    stats = df.groupby('trip_id').agg(
        placa=('placa', 'first'),
        data_inicio=('data_hora', 'min'),
        data_fim=('data_hora', 'max'),
        km_inicial=('odometro', 'first'),  # Primeiro odômetro (não mínimo - pode haver reset)
        km_final=('odometro', 'last'),      # Último odômetro
        lat_inicio=('latitude', 'first'),
        lon_inicio=('longitude', 'first'),
        lat_fim=('latitude', 'last'),
        lon_fim=('longitude', 'last'),
        qtd_pontos=('raw_id', 'count'),
        raw_id_inicio=('raw_id', 'min'),
        raw_id_fim=('raw_id', 'max'),
        tipo_parada=('tipo_gap', 'first'),  # Tipo do primeiro ponto da viagem
        ignicao_media=('ignicao', 'mean'),  # Para determinar se estava mais ligado ou desligado
    ).reset_index()
    
    print("⏱️ Calculando tempo ocioso e motor off por viagem...")
    tempos_ociosos = []
    tempos_motor_off = []
    
    for trip_id in stats['trip_id']:
        trip_df = df[df['trip_id'] == trip_id]
        
        # Tempo Ocioso (Motor Ligado + Vel 0)
        # Ajuste: Ociosidade deve considerar apenas ignicao=1?
        # A definição original era velocity < threshold.
        # Vamos refinar: Ocioso = Vel < limit E Ignicao = 1
        trip_df_ocioso = trip_df[trip_df['ignicao'] == 1]
        t_ocioso = calcular_tempo_ocioso(trip_df_ocioso)
        tempos_ociosos.append(t_ocioso)
        
        # Tempo Motor Off
        t_off = calcular_tempo_motor_off(trip_df)
        tempos_motor_off.append(t_off)
        
    stats['tempo_ocioso'] = tempos_ociosos
    stats['tempo_motor_off'] = tempos_motor_off
    
    # Calcular métricas derivadas
    stats['distancia'] = stats['km_final'] - stats['km_inicial']
    stats['tempo_minutos'] = (stats['data_fim'] - stats['data_inicio']).dt.total_seconds() / 60
    
    # Determinar situação baseada na ignição média
    stats['situacao'] = stats['ignicao_media'].apply(
        lambda x: 'MOVIMENTO' if x > 0.5 else 'PARADO'
    )
    
    # 5. Validar e filtrar viagens
    # Filtrar distâncias inválidas (negativas ou muito pequenas = manobra)
    stats_validas = stats[
        (stats['distancia'] >= MIN_DISTANCIA_VIAGEM) & 
        (stats['distancia'] < 2000)  # Max 2000km em uma viagem (sanity check)
    ].copy()
    
    # Viagens com distância negativa = reset de odômetro, marcar diferente
    stats_reset = stats[stats['distancia'] < 0].copy()
    if len(stats_reset) > 0:
        print(f"⚠️ {len(stats_reset)} viagens com possível reset de odômetro (distância negativa)")
        stats_reset['distancia'] = 0
        stats_reset['tipo_parada'] = 'RESET_ODOMETRO'
        # Incluir mesmo assim para não perder rastreabilidade
        stats_validas = pd.concat([stats_validas, stats_reset], ignore_index=True)
    
    print(f"✨ Viagens válidas após filtro: {len(stats_validas)}")
    
    # 6. Geocodificação e Inserção
    c = conn.cursor()
    trips_to_insert = []
    
    for idx, row in stats_validas.iterrows():
        local_inicio = get_cached_city_name(row['lat_inicio'], row['lon_inicio'])
        local_fim = get_cached_city_name(row['lat_fim'], row['lon_fim'])
        
        trips_to_insert.append((
            row['placa'],
            row['data_inicio'].strftime('%Y-%m-%d %H:%M:%S'),
            row['data_fim'].strftime('%Y-%m-%d %H:%M:%S'),
            float(row['km_inicial']),
            float(row['km_final']),
            float(row['distancia']),
            local_inicio,
            local_fim,
            float(row['tempo_minutos']),
            float(row['tempo_ocioso']),
            float(row['tempo_motor_off']),
            row['situacao'],
            row['tipo_parada'],
            int(row['qtd_pontos']),
            int(row['raw_id_inicio']),
            int(row['raw_id_fim']),
        ))
        
        if idx % 20 == 0:
            print(f"  Processando {idx}/{len(stats_validas)}: {row['placa']} - {local_inicio} -> {local_fim}")

    # 7. Inserir em Batch (sem DELETE prévio!)
    if trips_to_insert:
        ph_ins = get_placeholder(16)
        query_insert = f"""
            INSERT INTO deslocamentos 
            (placa, data_inicio, data_fim, km_inicial, km_final, distancia, 
             local_inicio, local_fim, tempo, tempo_ocioso, tempo_motor_off, situacao, 
             tipo_parada, qtd_pontos, raw_id_inicio, raw_id_fim, status)
            VALUES ({ph_ins}, 'PENDENTE')
        """
        c.executemany(query_insert, trips_to_insert)
        conn.commit()
        print(f"✅ Sucesso: {len(trips_to_insert)} viagens NOVAS inseridas no banco.")
    else:
        print("ℹ️ Nenhuma viagem nova para inserir.")
    
    conn.close()
    
    # Resumo final
    print("\n" + "="*50)
    print("📊 RESUMO DO PROCESSAMENTO")
    print("="*50)
    print(f"  Posições processadas: {len(df)}")
    print(f"  Viagens geradas: {len(trips_to_insert)}")
    if len(stats_validas) > 0:
        print(f"  Tipos de parada:")
        print(stats_validas['tipo_parada'].value_counts().to_string())


def limpar_e_reprocessar():
    """
    Limpa TODOS os deslocamentos pendentes e reprocessa do zero.
    USE COM CUIDADO - apenas quando necessário reconstruir tudo.
    """
    print("⚠️ ATENÇÃO: Limpando todos os deslocamentos PENDENTES...")
    
    conn = get_connection()
    c = conn.cursor()
    
    # Contar antes
    c.execute("SELECT COUNT(*) FROM deslocamentos WHERE status = 'PENDENTE'")
    qtd_antes = c.fetchone()[0]
    
    # Deletar apenas pendentes (não afeta processados/vinculados a viagens)
    c.execute("DELETE FROM deslocamentos WHERE status = 'PENDENTE'")
    conn.commit()
    conn.close()
    
    print(f"🗑️ {qtd_antes} deslocamentos pendentes removidos.")
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
        elif comando == "--limpar-duplicatas":
            remover_duplicatas()
        elif comando == "--corrigir-nomes":
            corrigir_nomes_locais()
        elif comando == "--help":
            print("""
Processador de Deslocamentos v3
================================
Uso: python processor.py [opção]

Opções:
  (sem opção)           Processamento incremental normal
  --reprocessar         Limpa pendentes e reprocessa tudo
  --limpar-duplicatas   Remove deslocamentos e posições duplicados
  --corrigir-nomes      Corrige nomes de locais verbosos
  --help                Mostra esta ajuda
            """)
        else:
            print(f"Opção desconhecida: {comando}")
            print("Use --help para ver as opções disponíveis")
    else:
        processar_deslocamentos()

