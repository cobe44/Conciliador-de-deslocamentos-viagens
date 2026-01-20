import math
import sqlite3
import requests
from datetime import datetime
from functools import lru_cache

try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False
    print("AVISO: geopy não instalado. Usando API IBGE como fallback.")

from poi_data import POIS_TATUI, POIS_PASSOS, POIS_IPIGUA, POIS_NUPORANGA

from database import get_connection, get_pois, get_placeholder

# Config
DB_NAME = "frota.db"

def get_connection():
    from database import get_connection as get_db_conn
    return get_db_conn()

def haversine(lat1, lon1, lat2, lon2):
    """
    Calcula a distancia em km entre dois pontos (lat/lon) usando a formula de Haversine.
    """
    R = 6371  # Raio da Terra em km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) * math.sin(dlat / 2) + \
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
        math.sin(dlon / 2) * math.sin(dlon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def get_pois():
    """Retorna todos os POIs do banco de dados."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, nome, latitude, longitude, tipo, raio FROM pois")
    pois = c.fetchall()
    conn.close()
    return pois

def get_poi_name(lat, lon, pois=None, pois_dict=None):
    """
    Retorna o nome do POI mais próximo das coordenadas fornecidas.
    Busca: 1) banco de dados, 2) dicionários de POIs, 3) reverse geocoding.
    
    Args:
        lat: Latitude
        lon: Longitude
        pois: Lista de POIs do banco [(id, nome, lat, lon, tipo, raio), ...]
        pois_dict: Dicionário de POIs locais {nome: [(lat, lon), ...], ...}
    
    Returns:
        Nome do POI, cidade, ou "Em Trânsito" se nenhum encontrado
    """
    # Buscar no banco de dados primeiro
    if pois is None:
        pois = get_pois()
    
    for p in pois:
        pid, pname, plat, plng, ptype, pradius = p
        dist = haversine(lat, lon, plat, plng) * 1000  # Converter para metros
        if dist <= pradius:
            return pname
    
    # Fallback: buscar nos dicionários de POIs locais
    if pois_dict:
        for name, coords_list in pois_dict.items():
            for (p_lat, p_lon) in coords_list:
                dist = haversine(lat, lon, p_lat, p_lon) * 1000
                if dist <= 1000:  # Raio padrão de 1km
                    return name
    
    # Tentar todos os dicionários de POIs (incluindo Nuporanga)
    all_pois = {**POIS_TATUI, **POIS_PASSOS, **POIS_IPIGUA, **POIS_NUPORANGA}
    for name, coords_list in all_pois.items():
        for (p_lat, p_lon) in coords_list:
            dist = haversine(lat, lon, p_lat, p_lon) * 1000
            if dist <= 1000:
                return name
    
    # Fallback: IBGE API primeiro (mais preciso para Brasil), depois geopy
    cidade = reverse_geocode_ibge(lat, lon)
    if cidade and cidade != "Em Trânsito":
        return cidade
    
    return reverse_geocode(lat, lon)


@lru_cache(maxsize=1000)
def reverse_geocode_ibge(lat, lon):
    """
    Usa API do IBGE para obter cidade/UF das coordenadas.
    Mais preciso para locais brasileiros.
    """
    try:
        # Arredondar para melhorar cache
        lat_round = round(lat, 4)
        lon_round = round(lon, 4)
        
        # API de localidades do IBGE
        url = f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        # Usar API de geocodificação reversa do IBGE não existe diretamente,
        # então usamos a API de posição geográfica via nominatim com dados IBGE
        
        # Alternativa: usar OpenStreetMap Nominatim com preferência por IBGE
        resp = requests.get(
            f"https://nominatim.openstreetmap.org/reverse",
            params={
                'lat': lat_round, 'lon': lon_round,
                'format': 'json', 'zoom': 10,
                'addressdetails': 1, 'accept-language': 'pt-BR'
            },
            headers={'User-Agent': 'FrotaCF/1.0'},
            timeout=5
        )
        
        if resp.status_code == 200:
            data = resp.json()
            addr = data.get('address', {})
            
            city = addr.get('city') or addr.get('town') or addr.get('municipality') or addr.get('village')
            state_name = addr.get('state', '')
            
            # Mapeamento de estados
            ESTADOS_BR = {
                'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM',
                'Bahia': 'BA', 'Ceará': 'CE', 'Distrito Federal': 'DF',
                'Espírito Santo': 'ES', 'Goiás': 'GO', 'Maranhão': 'MA',
                'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG',
                'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR', 'Pernambuco': 'PE',
                'Piauí': 'PI', 'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN',
                'Rio Grande do Sul': 'RS', 'Rondônia': 'RO', 'Roraima': 'RR',
                'Santa Catarina': 'SC', 'São Paulo': 'SP', 'Sergipe': 'SE', 'Tocantins': 'TO'
            }
            
            state_abbr = ESTADOS_BR.get(state_name, '')
            
            if city and state_abbr:
                return f"{city}/{state_abbr}"
            elif city:
                return city
        
        return "Em Trânsito"
    except Exception:
        return "Em Trânsito"


@lru_cache(maxsize=500)
def reverse_geocode(lat, lon):
    """
    Converte coordenadas em nome de cidade usando Nominatim.
    Usa cache para evitar chamadas repetidas.
    """
    if not GEOPY_AVAILABLE:
        return "Em Trânsito"
    
    # Mapeamento de estados brasileiros (nome completo → sigla)
    ESTADOS_BR = {
        'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM',
        'Bahia': 'BA', 'Ceará': 'CE', 'Distrito Federal': 'DF',
        'Espírito Santo': 'ES', 'Goiás': 'GO', 'Maranhão': 'MA',
        'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG',
        'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR', 'Pernambuco': 'PE',
        'Piauí': 'PI', 'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN',
        'Rio Grande do Sul': 'RS', 'Rondônia': 'RO', 'Roraima': 'RR',
        'Santa Catarina': 'SC', 'São Paulo': 'SP', 'Sergipe': 'SE', 'Tocantins': 'TO'
    }
    
    try:
        geolocator = Nominatim(user_agent="frota_cf_app", timeout=5)
        # Arredondar para reduzir variação e melhorar cache
        lat_round = round(lat, 3)
        lon_round = round(lon, 3)
        
        location = geolocator.reverse(f"{lat_round}, {lon_round}", language='pt')
        
        if location and location.raw.get('address'):
            addr = location.raw['address']
            # Tentar obter cidade/município
            city = addr.get('city') or addr.get('town') or addr.get('municipality') or addr.get('village')
            state_name = addr.get('state', '')
            
            # Converter nome do estado para sigla
            state_abbr = ESTADOS_BR.get(state_name, '')
            
            if city and state_abbr:
                return f"{city}/{state_abbr}"
            elif city:
                return city
            elif addr.get('road'):
                return addr.get('road')
        
        return "Em Trânsito"
    except (GeocoderTimedOut, GeocoderServiceError, Exception):
        return "Em Trânsito"

def processar_deslocamentos():
    """
    Processa posições brutas e cria deslocamentos baseados em odômetro.
    
    REGRA: Um deslocamento real só existe quando o odômetro muda.
    - MOVIMENTO: ignição ligada E odômetro mudou
    - PARADO: ignição ligada MAS odômetro não mudou (refrigeração)
    """
    conn = get_connection()
    c = conn.cursor()
    
    # Carregar POIs para performance
    pois = get_pois()
    
    # Obter todos os veículos com posições
    c.execute("""
        SELECT DISTINCT p.id_veiculo, v.placa 
        FROM posicoes_raw p
        LEFT JOIN veiculos v ON p.id_veiculo = v.id_sascar
        WHERE v.placa IS NOT NULL
    """)
    veiculos = c.fetchall()
    
    if not veiculos:
        print("Nenhum veículo com placa cadastrada encontrado.")
        conn.close()
        return
    
    total_deslocamentos = 0
    
    for id_veiculo, placa in veiculos:
        print(f"\nProcessando veículo: {placa} (ID: {id_veiculo})")
        
        # Verificar última posição já processada para este veículo
        ph1 = get_placeholder(1)
        c.execute(f"""
            SELECT MAX(data_fim) FROM deslocamentos WHERE placa = {ph1}
        """, (placa,))
        row = c.fetchone()
        ultima_data = row[0] if row and row[0] else None
        
        # Buscar posições ordenadas por data
        # Buscar posições ordenadas por data
        if ultima_data:
            ph2 = get_placeholder(2)
            c.execute(f"""
                SELECT id, data_hora, latitude, longitude, odometro, ignicao, velocidade
                FROM posicoes_raw 
                WHERE id_veiculo = {get_placeholder(1)} AND data_hora > {get_placeholder(1)}
                ORDER BY data_hora
            """, (id_veiculo, ultima_data))
        else:
            ph1 = get_placeholder(1)
            c.execute(f"""
                SELECT id, data_hora, latitude, longitude, odometro, ignicao, velocidade
                FROM posicoes_raw 
                WHERE id_veiculo = {ph1}
                ORDER BY data_hora
            """, (id_veiculo,))
        
        posicoes = c.fetchall()
        
        if not posicoes:
            print(f"  Sem novas posições para processar.")
            continue
        
        print(f"  {len(posicoes)} posições a processar...")
        
        # Estado da máquina de estados
        em_deslocamento = False
        deslocamento_atual = None
        ultima_pos_ignon = None
        tempo_ocioso = 0  # Segundos com ignição ligada e velocidade 0
        
        # Para consolidar PARADOs consecutivos
        parado_pendente = None  # Guarda um PARADO até confirmar que não vem mais
        
        # Tolerância para considerar movimento real (1.5km)
        TOLERANCIA_MOVIMENTO = 1.5
        
        def salvar_deslocamento(desloc_data, situacao):
            """Helper para salvar deslocamento no banco"""
            nonlocal total_deslocamentos
            ph_ins = get_placeholder(11)
            c.execute(f"""
                INSERT INTO deslocamentos 
                (placa, data_inicio, data_fim, km_inicial, km_final, distancia, 
                 local_inicio, local_fim, tempo, tempo_ocioso, situacao, status)
                VALUES ({ph_ins}, 'PENDENTE')
            """, (
                desloc_data['placa'],
                desloc_data['data_inicio'],
                desloc_data['data_fim'],
                desloc_data['km_inicial'],
                desloc_data['km_final'],
                desloc_data['distancia'],
                desloc_data['local_inicio'],
                desloc_data['local_fim'],
                desloc_data['tempo'],
                desloc_data['tempo_ocioso'],
                situacao
            ))
            total_deslocamentos += 1
            status_icon = '▶' if situacao == 'MOVIMENTO' else '⏸'
            print(f"  {status_icon} {situacao}: {desloc_data['local_fim']} ({desloc_data['distancia']:.1f}km, {desloc_data['tempo']:.0f}min)")
        
        for i, pos in enumerate(posicoes):
            pos_id, dt_str, lat, lon, odo, ign, vel = pos
            
            # Converter data
            if isinstance(dt_str, str):
                dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            else:
                dt = dt_str
            
            # Lógica de segmentação por ignição
            if ign == 1:  # Ignição ligada
                if not em_deslocamento:
                    # Início de um novo deslocamento
                    em_deslocamento = True
                    local_inicio = get_poi_name(lat, lon, pois)
                    deslocamento_atual = {
                        'placa': placa,
                        'data_inicio': dt,
                        'lat_inicio': lat,
                        'lon_inicio': lon,
                        'km_inicial': odo,
                        'local_inicio': local_inicio
                    }
                    tempo_ocioso = 0
                    print(f"  ▶ Início deslocamento em {dt.strftime('%d/%m %H:%M')} - {local_inicio}")
                
                # Atualizar última posição com ignição ligada
                ultima_pos_ignon = {
                    'data': dt,
                    'lat': lat,
                    'lon': lon,
                    'km': odo
                }
                
                # Calcular tempo ocioso (ignição ON + velocidade 0)
                if vel == 0 and i > 0:
                    prev_dt_str = posicoes[i-1][1]
                    if isinstance(prev_dt_str, str):
                        prev_dt = datetime.fromisoformat(prev_dt_str.replace('Z', '+00:00'))
                    else:
                        prev_dt = prev_dt_str
                    tempo_ocioso += (dt - prev_dt).total_seconds()
            
            elif ign == 0 and em_deslocamento:  # Ignição desligou
                # Fim do deslocamento
                if ultima_pos_ignon and deslocamento_atual:
                    local_fim = get_poi_name(
                        ultima_pos_ignon['lat'], 
                        ultima_pos_ignon['lon'], 
                        pois
                    )
                    
                    distancia = abs(ultima_pos_ignon['km'] - deslocamento_atual['km_inicial'])
                    tempo_deslocamento = (ultima_pos_ignon['data'] - deslocamento_atual['data_inicio']).total_seconds() / 60
                    
                    # Determinar situação: MOVIMENTO se houve deslocamento real (>1.5km)
                    situacao = 'MOVIMENTO' if distancia > TOLERANCIA_MOVIMENTO else 'PARADO'
                    
                    # Dados do deslocamento
                    desloc_data = {
                        'placa': placa,
                        'data_inicio': deslocamento_atual['data_inicio'].isoformat(),
                        'data_fim': ultima_pos_ignon['data'].isoformat(),
                        'km_inicial': deslocamento_atual['km_inicial'],
                        'km_final': ultima_pos_ignon['km'],
                        'distancia': distancia,
                        'local_inicio': deslocamento_atual['local_inicio'],
                        'local_fim': local_fim,
                        'tempo': tempo_deslocamento,
                        'tempo_ocioso': tempo_ocioso / 60
                    }
                    
                    if situacao == 'MOVIMENTO':
                        # Antes de salvar MOVIMENTO, salvar PARADO pendente se existir
                        if parado_pendente:
                            salvar_deslocamento(parado_pendente, 'PARADO')
                            parado_pendente = None
                        
                        # Salvar MOVIMENTO
                        salvar_deslocamento(desloc_data, 'MOVIMENTO')
                    else:
                        # PARADO: consolidar com pendente se existir
                        if parado_pendente:
                            # Extender o PARADO pendente
                            parado_pendente['data_fim'] = desloc_data['data_fim']
                            parado_pendente['km_final'] = desloc_data['km_final']
                            parado_pendente['distancia'] += desloc_data['distancia']
                            parado_pendente['tempo'] += desloc_data['tempo']
                            parado_pendente['tempo_ocioso'] += desloc_data['tempo_ocioso']
                            parado_pendente['local_fim'] = desloc_data['local_fim']
                        else:
                            # Novo PARADO pendente
                            parado_pendente = desloc_data
                
                # Reset estado
                em_deslocamento = False
                deslocamento_atual = None
                ultima_pos_ignon = None
                tempo_ocioso = 0
        
        # Ao final, salvar PARADO pendente se existir
        if parado_pendente:
            salvar_deslocamento(parado_pendente, 'PARADO')
            parado_pendente = None
        
        # Se ainda está em deslocamento ao final, não fecha (viagem em andamento)
        if em_deslocamento:
            print(f"  ⚠ Deslocamento em andamento (não fechado)")
    
    conn.commit()
    conn.close()
    
    print(f"\n{'='*50}")
    print(f"Processamento concluído! {total_deslocamentos} deslocamentos criados.")
    print(f"{'='*50}")


# Manter função antiga para compatibilidade (deprecated)
def process_data():
    """
    DEPRECATED: Use processar_deslocamentos() ao invés.
    Esta função será removida em versões futuras.
    """
    print("AVISO: process_data() está deprecada. Use processar_deslocamentos()")
    processar_deslocamentos()


if __name__ == "__main__":
    processar_deslocamentos()
