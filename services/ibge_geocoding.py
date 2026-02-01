"""
IBGE Geocoding Service - V2
===========================
Uses the BR_Municipios_2024.shp shapefile to find municipality names from coordinates.
Loads shapefile once at module initialization for fast lookups.
"""

import os
from functools import lru_cache

# Global variables
_gdf = None
_loaded = False

import requests
import zipfile
import io

def _download_and_extract(base_dir):
    """Download and extract IBGE shapefile."""
    url = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2023/Brasil/BR_Municipios_2023.zip"
    zip_path = os.path.join(base_dir, '..', 'utils', 'BR_Municipios_2023.zip')
    extract_path = os.path.join(base_dir, '..', 'utils')
    
    # Create utils dir if not exists
    os.makedirs(extract_path, exist_ok=True)
    
    print(f"[IBGE] Downloading map data from IBGE (approx 130MB)...")
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print("[IBGE] Download complete. Extracting...")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
            
        print("[IBGE] Extraction complete.")
    except Exception as e:
        print(f"[IBGE] Failed to download/extract: {e}")

def _load_shapefile():
    """Load IBGE shapefile on first use."""
    global _gdf, _loaded
    
    if _loaded:
        return _gdf
    
    try:
        import geopandas as gpd
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Trying 2023 (official link) or 2024 (if local)
        shapefile_path_23 = os.path.join(current_dir, '..', 'utils', 'BR_Municipios_2023.shp')
        shapefile_path_24 = os.path.join(current_dir, '..', 'utils', 'BR_Municipios_2024.shp')
        
        shapefile_path = shapefile_path_24 if os.path.exists(shapefile_path_24) else shapefile_path_23
        
        # Download if neither exists
        if not os.path.exists(shapefile_path):
            _download_and_extract(current_dir)
            shapefile_path = shapefile_path_23 # Default after download
            
        if not os.path.exists(shapefile_path):
            print(f"[IBGE] Shapefile still not found after download attempt.")
            _loaded = True
            return None
        
        print(f"[IBGE] Loading shapefile from {shapefile_path}...")
        _gdf = gpd.read_file(shapefile_path)
        
        # Ensure WGS84 coordinate system
        if _gdf.crs is None:
            _gdf.set_crs(epsg=4326, inplace=True)
        elif _gdf.crs.to_epsg() != 4326:
            _gdf = _gdf.to_crs(epsg=4326)
        
        # Create spatial index for fast lookups
        _gdf.sindex
        
        print(f"[IBGE] Loaded {len(_gdf)} municipalities successfully!")
        _loaded = True
        return _gdf
        
    except ImportError:
        print("[IBGE] ERROR: geopandas not installed. Run: pip install geopandas")
        _loaded = True
        return None
    except Exception as e:
        print(f"[IBGE] ERROR loading shapefile: {e}")
        _loaded = True
        return None


@lru_cache(maxsize=20000)
def get_municipio_ibge(lat: float, lon: float) -> str:
    """
    Get municipality name from coordinates using IBGE shapefile.
    Uses spatial index for fast point-in-polygon lookup.
    
    Args:
        lat: Latitude (float)
        lon: Longitude (float)
        
    Returns:
        "CityName/UF" or "(lat, lon)" if not found
    """
    try:
        lat = round(float(lat), 4)
        lon = round(float(lon), 4)
    except (ValueError, TypeError):
        return f"({lat}, {lon})"
    
    gdf = _load_shapefile()
    if gdf is None or gdf.empty:
        return f"({lat}, {lon})"
    
    try:
        from shapely.geometry import Point
        
        # Create point (note: lon=x, lat=y)
        point = Point(lon, lat)
        
        # Use spatial index for faster query
        possible_matches_index = list(gdf.sindex.intersection(point.bounds))
        
        if possible_matches_index:
            possible_matches = gdf.iloc[possible_matches_index]
            precise_matches = possible_matches[possible_matches.geometry.contains(point)]
            
            if not precise_matches.empty:
                row = precise_matches.iloc[0]
                nome = row.get('NM_MUN', '')
                uf = row.get('SIGLA_UF', '')
                
                if nome:
                    return f"{nome}/{uf}" if uf else nome
        
        # Fallback - check all polygons (slower but more thorough)
        for idx, row in gdf.iterrows():
            if row.geometry.contains(point):
                nome = row.get('NM_MUN', '')
                uf = row.get('SIGLA_UF', '')
                if nome:
                    return f"{nome}/{uf}" if uf else nome
        
        return f"({lat}, {lon})"
        
    except Exception as e:
        return f"({lat}, {lon})"


def is_coordinate_string(s: str) -> bool:
    """Check if a string looks like a coordinate."""
    if not s or not isinstance(s, str):
        return False
    s = s.strip()
    # Check for (lat, lon) format
    if s.startswith('(') and s.endswith(')'):
        return True
    # Check for lat, lon format
    if ',' in s:
        parts = s.split(',')
        if len(parts) == 2:
            try:
                float(parts[0].strip())
                float(parts[1].strip())
                return True
            except:
                pass
    return False


def parse_coordinates(s: str) -> tuple:
    """Parse a coordinate string into (lat, lon) tuple."""
    if not s:
        return None
    s = s.strip().strip('()').strip()
    parts = s.split(',')
    if len(parts) == 2:
        try:
            lat = float(parts[0].strip())
            lon = float(parts[1].strip())
            return (lat, lon)
        except:
            pass
    return None


# Test function
if __name__ == "__main__":
    # Test with some known coordinates
    test_coords = [
        (-23.5505, -46.6333),  # São Paulo
        (-22.9068, -43.1729),  # Rio de Janeiro
        (-20.48, -54.63),      # Campo Grande
        (-15.77, -47.92),      # Brasília
        (-19.92, -43.94),      # Belo Horizonte
    ]
    
    for lat, lon in test_coords:
        result = get_municipio_ibge(lat, lon)
        print(f"({lat}, {lon}) -> {result}")
