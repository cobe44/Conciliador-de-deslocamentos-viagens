from database import get_connection, get_placeholder, IS_POSTGRES
from poi_data import POIS_TATUI, POIS_PASSOS, POIS_IPIGUA

def clean_and_seed_pois():
    conn = get_connection()
    c = conn.cursor()
    
    # Opcional: Esvaziar tabela de POIs antes de reinserir (cuidado com IDs de viagens existentes)
    # Como o sistema é novo, vamos limpar para garantir que os dados estejam frescos
    # Para produção, seria melhor um UPSERT (Update if exists)
    print(" Limpando tabela de POIs antiga...")
    c.execute("DELETE FROM pois") 
    
    # resetar autoincrement (apenas SQLite)
    if not IS_POSTGRES:
        c.execute("UPDATE sqlite_sequence SET seq = 0 WHERE name = 'pois'")

    # Função Helper
    def insert_poi(name, coords_list, radius, ptype):
        # coords_list é uma lista de tuplas (lat, lon). Vamos pegar a primeira para simplificar o centro.
        # Se houver mais de uma coordenada, idealmente calcular o centroide ou criar multiplos POIs.
        # O user mandou listas como [(-23.33, -47.84), ...]. Vamos usar a primeira.
        lat, lon = coords_list[0]
        
        # Tentar inferir tipo se não passado explícito, mas aqui vamos usar um generico 'Ponto'
        # ou inferir pelo nome.
        final_type = ptype
        if not final_type:
            lower_name = name.lower()
            if "base" in lower_name: final_type = "Base"
            elif "granja" in lower_name or "sitio" in lower_name or "fazenda" in lower_name: final_type = "Granja"
            elif "oficina" in lower_name: final_type = "Oficina"
            elif "posto" in lower_name: final_type = "Posto"
            elif "jbs" in lower_name: final_type = "Base" # JBS atua como base/incubatorio
            else: final_type = "Granja" # Default para a maioria dos pontos rurais

        ph = get_placeholder(5)
        c.execute(f"INSERT INTO pois (nome, latitude, longitude, tipo, raio) VALUES ({ph})", 
                  (name, lat, lon, final_type, radius))

    print(" Inserindo POIs Tatuí (Raio Base 3000m, Outros 600m)...")
    for name, coords in POIS_TATUI.items():
        radius = 3000 if "Base" in name else 600
        # Check special cases if needed
        insert_poi(name, coords, radius, None)

    print(" Inserindo POIs Passos (Raio 120m)...")
    for name, coords in POIS_PASSOS.items():
        # Regra especifica user: Locais em Passos 120m
        insert_poi(name, coords, 120, None)

    print(" Inserindo POIs Ipiguá (Raio 600m Default)...")
    for name, coords in POIS_IPIGUA.items():
        insert_poi(name, coords, 600, "Base") # Incubatorio Ipiguá parece ser um ponto central

    conn.commit()
    
    # Verificar total
    c.execute("SELECT count(*) FROM pois")
    total = c.fetchone()[0]
    print(f"Total de POIs importados: {total}")
    
    conn.close()

if __name__ == "__main__":
    clean_and_seed_pois()
