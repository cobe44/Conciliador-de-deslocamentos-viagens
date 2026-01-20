import pandas as pd
import numpy as np
import sqlite3
import requests
from datetime import datetime
from functools import lru_cache
from database import get_connection, get_placeholder, get_pois

# Usar apenas a base
from poi_data import POIS_NUPORANGA

# Constantes
GAP_THRESHOLD_MINUTES = 20  # Tempo sem sinal (ou parado) para considerar nova viagem
STOP_THRESHOLD_KMH = 3      # Velocidade abaixo disto é considerado parado
MIN_DISTANCIA_VIAGEM = 1.0  # Viagens menores que 1km são descartadas (ruído/manobra)

# Configuração Geopy
try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False


@lru_cache(maxsize=2000)
def get_cached_city_name(lat, lon):
    """
    Geocodificação reversa com cache e arredondamento para maximizar hits.
    Arredonda para 3 casas (aprox 100m) para agrupar locais próximos.
    """
    lat_r = round(lat, 3)
    lon_r = round(lon, 3)
    
    # 1. Tentar Base Nuporanga
    for name, coords_list in POIS_NUPORANGA.items():
        for (p_lat, p_lon) in coords_list:
            # Distancia euclidiana simples para rapidez (check preliminar)
            if abs(lat_r - p_lat) < 0.01 and abs(lon_r - p_lon) < 0.01:
                return name

    # 2. OpenStreetMap / IBGE (Simulado via Nominatim com tratamento)
    if not GEOPY_AVAILABLE:
        return "Local Desconhecido"
        
    try:
        geolocator = Nominatim(user_agent="frota_cf_v2", timeout=3)
        loc = geolocator.reverse(f"{lat_r}, {lon_r}", language='pt')
        if loc and loc.address:
            address = loc.raw.get('address', {})
            city = address.get('city') or address.get('town') or address.get('municipality') or address.get('village')
            state = address.get('state')
            
            # Formatar Estado
            est_map = {
                'São Paulo': 'SP', 'Minas Gerais': 'MG', 'Goiás': 'GO', 'Paraná': 'PR',
                'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Bahia': 'BA'
                # Adicionar outros conforme necessidade
            }
            uf = est_map.get(state, state) if state else ""
            
            if city:
                return f"{city}/{uf}" if uf and len(uf) == 2 else city
            return "Em Trânsito"
            
    except Exception as e:
        pass
        
    return f"{lat_r}, {lon_r}"

def processar_deslocamentos():
    """
    Versão V2 Otimizada e Vetorizada.
    Processa todos os veículos de uma vez usando Pandas.
    Lógica: Viagem é definida por GAPS de tempo.
    """
    print("🚀 Iniciando Processador V2 (Pandas Vectorized)...")
    conn = get_connection()
    
    # 1. Carregar Dados Brutos (Apenas colunas necessárias)
    # Pegar apenas dados novos seria ideal, mas para garantir consistência vamos pegar 
    # dos últimos X dias ou tudo se for primeira vez. 
    # Por simplicidade e robustez, pegamos tudo ordenado, mas em produção filtraríamos > last_processed_id.
    
    query = """
        SELECT 
            p.id, 
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
        ORDER BY p.id_veiculo, p.data_hora
    """
    
    # Ler tudo para memmória (Pandas é eficiente, até ~1M linhas ok)
    df = pd.read_sql(query, conn)
    
    if df.empty:
        print("Nenhum dado para processar.")
        conn.close()
        return

    # Converter data
    df['data_hora'] = pd.to_datetime(df['data_hora'])
    
    # 2. Identificar GAPS e Mudanças de Veículo
    # Calcula diferença de tempo entre linhas consecutivas
    df['time_diff'] = df.groupby('placa')['data_hora'].diff().dt.total_seconds() / 60
    
    # Nova Viagem se:
    # 1. Mudou o carro (primeira linha do grupo time_diff é NaT)
    # 2. Gap de tempo > LIMIAR (Ex: 20 min sem sinal ou parado desligado)
    df['new_trip'] = (df['time_diff'] > GAP_THRESHOLD_MINUTES) | (df['time_diff'].isna())
    
    # Criar ID único de viagem (Cumulative Sum dos flags de nova viagem)
    df['trip_id'] = df['new_trip'].cumsum()
    
    print(f"📦 Dados carregados: {len(df)} linhas. Viagens brutas identificadas: {df['trip_id'].max()}")
    
    # 3. Agregar dados por Viagem (Vectorized Aggregation)
    # Aqui a mágica acontece instantaneamente
    stats = df.groupby('trip_id').agg(
        placa=('placa', 'first'),
        data_inicio=('data_hora', 'min'),
        data_fim=('data_hora', 'max'),
        km_inicial=('odometro', 'min'),
        km_final=('odometro', 'max'),
        lat_inicio=('latitude', 'first'),
        lon_inicio=('longitude', 'first'),
        lat_fim=('latitude', 'last'),
        lon_fim=('longitude', 'last'),
        count=('id', 'count')
    ).reset_index()
    
    # Calcular métricas derivadas
    stats['distancia'] = stats['km_final'] - stats['km_inicial']
    stats['tempo_minutos'] = (stats['data_fim'] - stats['data_inicio']).dt.total_seconds() / 60
    
    # Filtrar viagens insignificantes (manobras de pátio, ruído)
    # Ex: andou menos de 1km
    viagens_validas = stats[stats['distancia'] > MIN_DISTANCIA_VIAGEM].copy()
    
    print(f"✨ Viagens válidas após filtro: {len(viagens_validas)}")
    
    # 4. Geocodificação (O gargalo agora é aqui, mas reduzimos de 100k pontos para ~X viagens)
    # Fazer geocoding apenas do inicio e fim
    
    # Limpar tabela atual para substituir pelos dados processados limpos
    # (Ou fazer merge inteligente. Aqui vamos limpar os PENDENTES para re-popular com a nova lógica)
    c = conn.cursor()
    c.execute("DELETE FROM deslocamentos WHERE status = 'PENDENTE'")
    conn.commit()
    
    trips_to_insert = []
    
    # Iterar apenas nas viagens agregadas (muito menos que as linhas brutas)
    for idx, row in viagens_validas.iterrows():
        # Calcular tempo parado dentro da viagem (refinamento)
        # Pegar as linhas originais dessa trip
        # mask = df['trip_id'] == row['trip_id']
        # sub_df = df[mask]
        # tempo_parado = sub_df[sub_df['velocidade'] < STOP_THRESHOLD_KMH]['time_diff'].sum() 
        # (Isso seria preciso, mas lento. Vamos estimar ou deixar 0 por enquanto para performance extrema)
        # Estimativa simples: se a velocidade média for muito baixa? Não, melhor deixar 0.
        
        # Geocoding
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
            0.0, # Tempo ocioso (ignorado v1)
            'MOVIMENTO'
        ))
        
        if idx % 10 == 0:
            print(f"  Geocoding {idx}/{len(viagens_validas)}: {local_inicio} -> {local_fim}")

    # 5. Inserir em Batch (Bulk Insert)
    if trips_to_insert:
        ph_ins = get_placeholder(11)
        query_insert = f"""
            INSERT INTO deslocamentos 
            (placa, data_inicio, data_fim, km_inicial, km_final, distancia, 
             local_inicio, local_fim, tempo, tempo_ocioso, situacao, status)
            VALUES ({ph_ins}, 'PENDENTE')
        """
        c.executemany(query_insert, trips_to_insert)
        conn.commit()
        print(f"✅ Sucesso: {len(trips_to_insert)} viagens inseridas no banco.")
    
    conn.close()

if __name__ == "__main__":
    processar_deslocamentos()
