"""
Pontos de Interesse (POIs) - Locais Fixos
==========================================
Quando um caminhão estiver dentro do raio de 600m de um POI,
o local será identificado pelo nome do POI.

Raio padrão: 600m (~0.0054 graus)
"""

# Raio em graus (aproximadamente 600m)
# 1 grau de latitude ≈ 111km, então 600m ≈ 0.0054 graus
POI_RADIUS = 0.006  # ~660m para margem de segurança

# ==========================================
# POIS CADASTRADOS
# ==========================================
POIS_NUPORANGA = {
    # Base Andrioni Original
    "Base Nuporanga (Andrioni)": [(-20.722612, -47.751135)],
    
    # Linha Granjas Faria (MT)
    "Linha Granjas Faria": [(-14.016888, -56.085139)],
    
    # JBS Nuporanga/SP (2 pontos)
    "JBS Nuporanga/SP": [
        (-20.737595, -47.768771),
        (-20.73766521696245, -47.769079458847585)
    ],
    
    # JBS Passos/MG (2 pontos)
    "JBS Passos/MG": [
        (-20.732566, -46.572874),
        (-20.731582, -46.572266)
    ],
    
    # Incubatório Ipigua/SP
    "Incubatório Ipigua/SP": [(-20.653082, -49.387978)],
    
    # Incubatório Sarapui/SP
    "Incubatório Sarapui/SP": [(-23.620574, -47.825299)],
    
    # Incubatório Pereiras/SP
    "Incubatório Pereiras/SP": [(-23.053194, -47.961408)],
    
    # JBS Tatuí/SP
    "JBS Tatuí/SP": [(-23.37935, -47.784383)],
    
    # JBS Sidrolândia/MS
    "JBS Sidrolândia/MS": [(-20.910013, -54.946136)],
    
    # Incubatório Nuporanga/SP
    "Incubatório Nuporanga/SP": [(-20.739045, -47.748742)],
    
    # Incubatório Brasília/DF
    "Incubatório Brasília/DF": [(-15.938229, -48.16641)],
    
    # Incubatório São Gonçalo dos Campos/BA
    "Incubatório São Gonçalo dos Campos/BA": [(-12.304453, -38.958221)],
}

# Dicionários vazios para compatibilidade
POIS_TATUI = {}
POIS_PASSOS = {}
POIS_IPIGUA = {}
MAPA_CIDADES_PASSOS = {}
