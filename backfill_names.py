# Script para preencher nomes de locais em background
# Evita timeouts no processamento principal rodando de forma controlada

import time
import logging
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from database import get_connection

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def limpar_nome_local(nome):
    """Limpa nomes verbosos"""
    if not nome: return nome
    
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

def geocode_latlon(geolocator, lat, lon):
    """Geocodifica com retries e tratamento de erro"""
    # Se lat/lon já forem inválidos
    try:
        lat = float(lat)
        lon = float(lon)
    except:
        return None
        
    try:
        # Tentar geocodificar
        loc = geolocator.reverse(f"{lat}, {lon}", language='pt')
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
    except Exception as e:
        logger.warning(f"Erro ao geocodificar {lat}, {lon}: {e}")
    
    return None

def parse_lat_lon(local_str):
    """Extrai lat/lon de uma string como '-23.123, -46.456'"""
    try:
        parts = local_str.split(',')
        if len(parts) == 2:
            lat = float(parts[0].strip())
            lon = float(parts[1].strip())
            # Validação básica de coordenada geográfica
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon
    except:
        pass
    return None, None

def processar_backfill():
    conn = get_connection()
    c = conn.cursor()
    
    # 1. Buscar locais que parecem coordenadas (contêm vírgula e números, menos de 40 chars)
    # Ignora locais que já têm barra (Cidade/UF) ou letras (nomes de ruas/POIs)
    logger.info("🔍 Buscando locais pendentes de geocodificação...")
    
    # Busca apenas ID e locais
    query = """
    SELECT id, local_inicio, local_fim 
    FROM deslocamentos 
    WHERE (local_inicio LIKE '%,%' AND length(local_inicio) < 40 AND local_inicio NOT LIKE '%/%' AND local_inicio ~ '^-?[0-9]')
       OR (local_fim LIKE '%,%' AND length(local_fim) < 40 AND local_fim NOT LIKE '%/%' AND local_fim ~ '^-?[0-9]')
    ORDER BY id DESC
    LIMIT 1000
    """
    
    try:
        c.execute(query)
    except Exception as e:
        # Fallback para SQLite ou se regex falhar
        conn.rollback()
        query = """
        SELECT id, local_inicio, local_fim 
        FROM deslocamentos 
        WHERE (local_inicio LIKE '%,%' AND length(local_inicio) < 40 AND local_inicio NOT LIKE '%/%')
           OR (local_fim LIKE '%,%' AND length(local_fim) < 40 AND local_fim NOT LIKE '%/%')
        ORDER BY id DESC
        LIMIT 1000
        """
        c = conn.cursor()
        c.execute(query)
        
    pendentes = c.fetchall()
    
    if not pendentes:
        logger.info("✅ Nenhum local pendente para geocodificar.")
        conn.close()
        return

    logger.info(f"📦 Encontrados {len(pendentes)} registros para verificar.")
    
    geolocator = Nominatim(user_agent="frota_backfill_v1", timeout=5)
    atualizados = 0
    
    for row in pendentes:
        did, loc_ini, loc_fim = row
        mudou = False
        novo_ini = loc_ini
        novo_fim = loc_fim
        
        # Verificar inicio
        lat_i, lon_i = parse_lat_lon(loc_ini)
        if lat_i is not None:
            nome = geocode_latlon(geolocator, lat_i, lon_i)
            if nome:
                novo_ini = nome
                mudou = True
                logger.info(f"📍 ID {did} INÍCIO: {loc_ini} -> {nome}")
                # Rate limit amigável
                time.sleep(1.5)
        
        # Verificar fim
        lat_f, lon_f = parse_lat_lon(loc_fim)
        if lat_f is not None:
            nome = geocode_latlon(geolocator, lat_f, lon_f)
            if nome:
                novo_fim = nome
                mudou = True
                logger.info(f"📍 ID {did} FIM: {loc_fim} -> {nome}")
                time.sleep(1.5)
        
        if mudou:
            try:
                c.execute("""
                    UPDATE deslocamentos 
                    SET local_inicio = %s, local_fim = %s
                    WHERE id = %s
                """, (novo_ini, novo_fim, did))
                conn.commit()
                atualizados += 1
            except Exception as e:
                logger.error(f"Erro ao atualizar ID {did}: {e}")
                
    conn.close()
    logger.info(f"✨ Processo concluído. {atualizados} registros atualizados.")

if __name__ == "__main__":
    try:
        processar_backfill()
    except KeyboardInterrupt:
        logger.info("🛑 Interrompido pelo usuário.")
