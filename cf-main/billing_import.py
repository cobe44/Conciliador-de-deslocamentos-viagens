import os
import xml.etree.ElementTree as ET
from datetime import datetime
import sqlite3
from database import get_connection

BILLING_DIR = "faturas_xml"

def ensure_billing_dir_and_mock_data():
    if not os.path.exists(BILLING_DIR):
        os.makedirs(BILLING_DIR)
    
    # Criar um XML de exemplo se não existir
    mock_file = os.path.join(BILLING_DIR, "fatura_12345.xml")
    if not os.path.exists(mock_file):
        with open(mock_file, "w") as f:
            f.write("""
<FaturaTransporte>
    <Numero>12345</Numero>
    <Viagens>
        <Viagem>
            <IdVeiculo>101</IdVeiculo>
            <Data>2023-10-27</Data>
            <Origem>Base SP</Origem>
            <Destino>Granja Modelo</Destino>
            <DistanciaKm>5.2</DistanciaKm>
            <ValorFrete>150.00</ValorFrete>
        </Viagem>
    </Viagens>
</FaturaTransporte>
            """)
        print(f"Arquivo de mock criado: {mock_file}")

def import_billing_xmls():
    ensure_billing_dir_and_mock_data()
    
    conn = get_connection()
    c = conn.cursor()
    
    files = [f for f in os.listdir(BILLING_DIR) if f.endswith('.xml')]
    print(f"Encontrados {len(files)} arquivos de faturamento.")
    
    for filename in files:
        filepath = os.path.join(BILLING_DIR, filename)
        try:
            tree = ET.parse(filepath)
            root = tree.getroot()
            
            for viagem in root.findall('.//Viagem'):
                # Extração dados
                vid = int(viagem.find('IdVeiculo').text)
                data_str = viagem.find('Data').text # YYYY-MM-DD
                # orig = viagem.find('Origem').text
                # dest = viagem.find('Destino').text
                km_faturado = float(viagem.find('DistanciaKm').text)
                
                # Tentar encontrar a viagem correspondente no banco de dados.
                # Simplificação: Match por Veiculo e Data (mesmo dia)
                # Na realidade precisaria de logica temporal mais refinada (dentro do intervalo da viagem)
                
                print(f"Processando fatura para Veiculo {vid} em {data_str} valor {km_faturado}km")
                
                # Update na tabela viagens
                # Procura viagem aberta ou fechada desse veiculo que intercepte essa data
                # Como simplificacao, vamos buscar viagens que começaram nesse dia
                
                # SQLite date() function helps matching YYYY-MM-DD strings
                c.execute('''
                    UPDATE viagens 
                    SET distancia_faturada = ? 
                    WHERE id_veiculo = ? 
                    AND date(data_inicio) = ?
                ''', (km_faturado, vid, data_str))
                
                if c.rowcount > 0:
                    print(f"  -> {c.rowcount} viagem(ns) atualizada(s) com km faturado.")
                else:
                    print(f"  -> Nenhuma viagem encontrada no sistema para vincular em {data_str}.")
                    
        except Exception as e:
            print(f"Erro ao processar {filename}: {e}")
            
    conn.commit()
    conn.close()

if __name__ == "__main__":
    import_billing_xmls()
