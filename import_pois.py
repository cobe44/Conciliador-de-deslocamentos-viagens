"""
Script de importação de POIs a partir de ficheiro Excel ou CSV.
Formato esperado: Nome, Latitude, Longitude, Raio, Tipo
"""
import pandas as pd
import sys
from database import get_connection

def importar_pois_excel(caminho_ficheiro):
    """
    Importa POIs de um ficheiro Excel ou CSV.
    
    Args:
        caminho_ficheiro: Caminho para o ficheiro .xlsx ou .csv
    """
    print(f"Lendo ficheiro: {caminho_ficheiro}")
    
    # Detectar formato
    if caminho_ficheiro.endswith('.csv'):
        df = pd.read_csv(caminho_ficheiro)
    else:
        df = pd.read_excel(caminho_ficheiro)
    
    # Validar colunas
    colunas_esperadas = ['Nome', 'Latitude', 'Longitude', 'Raio', 'Tipo']
    colunas_faltantes = [col for col in colunas_esperadas if col not in df.columns]
    
    if colunas_faltantes:
        print(f"ERRO: Colunas faltantes no ficheiro: {', '.join(colunas_faltantes)}")
        print(f"Colunas encontradas: {', '.join(df.columns)}")
        return False
    
    # Validar tipos de POI
    tipos_validos = ['Base', 'Granja', 'Oficina', 'Concessionaria', 'Posto']
    tipos_invalidos = df[~df['Tipo'].isin(tipos_validos)]['Tipo'].unique()
    
    if len(tipos_invalidos) > 0:
        print(f"AVISO: Tipos inválidos encontrados (serão ignorados): {', '.join(tipos_invalidos)}")
        df = df[df['Tipo'].isin(tipos_validos)]
    
    # Conectar ao banco
    conn = get_connection()
    c = conn.cursor()
    
    # Opção de limpar POIs existentes
    resposta = input("\nDeseja limpar POIs existentes antes de importar? (s/N): ").strip().lower()
    if resposta == 's':
        c.execute("DELETE FROM pois")
        c.execute("DELETE FROM sqlite_sequence WHERE name='pois'")
        print("POIs existentes removidos.")
    
    # Inserir POIs
    registros_inseridos = 0
    for idx, row in df.iterrows():
        try:
            c.execute("""
                INSERT INTO pois (nome, latitude, longitude, tipo, raio) 
                VALUES (?, ?, ?, ?, ?)
            """, (row['Nome'], row['Latitude'], row['Longitude'], row['Tipo'], int(row['Raio'])))
            registros_inseridos += 1
        except Exception as e:
            print(f"Erro ao inserir linha {idx+1}: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"\n✓ Importação concluída! {registros_inseridos} POIs inseridos no banco de dados.")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python import_pois.py <caminho_ficheiro.xlsx|csv>")
        print("\nFormato esperado do ficheiro:")
        print("  Colunas: Nome, Latitude, Longitude, Raio, Tipo")
        print("  Tipos válidos: Base, Granja, Oficina, Concessionaria, Posto")
        sys.exit(1)
    
    caminho = sys.argv[1]
    importar_pois_excel(caminho)
