import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv
from database import get_connection, get_placeholder, IS_POSTGRES

# Carregar variaveis do arquivo .env
load_dotenv()

class SascarClient:
    def __init__(self, user=None, password=None):
        self.user = user or os.getenv("SASCAR_USER")
        self.password = password or os.getenv("SASCAR_PASS")
        # Endpoint de serviço (sem ?wsdl)
        self.url = "https://sasintegra.sascar.com.br/SasIntegra/SasIntegraWSService"

    def _call_soap(self, method, params_xml):
        """Método genérico para chamadas SOAP."""
        if not self.user or not self.password:
            print("Erro: Credenciais não configuradas no .env ou via parâmetro.")
            return None

        soap_envelope = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:web="http://webservice.web.integracao.sascar.com.br/">
   <soapenv:Header/>
   <soapenv:Body>
      <web:{method}>
         <usuario>{self.user}</usuario>
         <senha>{self.password}</senha>
         {params_xml}
      </web:{method}>
   </soapenv:Body>
</soapenv:Envelope>"""

        headers = {
            "Content-Type": "text/xml;charset=UTF-8",
            "SOAPAction": ""
        }

        try:
            print(f"Enviando requisição para {method}...")
            response = requests.post(self.url, data=soap_envelope, headers=headers, timeout=60)
            if response.status_code != 200:
                print(f"Erro HTTP {response.status_code}")
                # Tentar extrair faultstring se for erro 500
                try:
                    if "faultstring" in response.text:
                        fault = response.text.split("<faultstring>")[1].split("</faultstring>")[0]
                        print(f"Detalhe do Erro: {fault}")
                    else:
                        print(f"Resposta do Servidor:\n{response.text[:500]}")
                except:
                    print(f"Resposta do Servidor (Erro ao ler XML):\n{response.text[:500]}")
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Erro na requisição Sascar: {e}")
            return None

    def obter_pacote_posicoes_com_placa(self, quantidade=0):
        """Obtém posições COM a placa do veículo inclusa no retorno."""
        return self._call_soap("obterPacotePosicoesComPlaca", f"<quantidade>{quantidade}</quantidade>")

    def obter_pacote_posicao_historico(self, id_veiculo, data_inicio, data_final):
        """
        Obtém histórico de posições por veículo e período.
        Formato de data esperado pela API: dd/MM/yyyy HH:mm:ss (ex: 10/01/2026 00:00:00)
        """
        params = f"""<dataInicio>{data_inicio}</dataInicio>
         <dataFinal>{data_final}</dataFinal>
         <idVeiculo>{id_veiculo}</idVeiculo>"""
        return self._call_soap("obterPacotePosicaoHistorico", params)

    def obter_veiculos(self, quantidade=0, id_veiculo=0):
        """Obtém a lista de veículos cadastrados no usuário."""
        return self._call_soap("obterVeiculos", f"<quantidade>{quantidade}</quantidade><idVeiculo>{id_veiculo}</idVeiculo>")

    def parse_positions_xml(self, xml_content):
        """Faz o parse do XML de retorno da Sascar (SOAP Response)."""
        if not xml_content:
            return []

        try:
            root = ET.fromstring(xml_content.strip())
            posicoes = []
            
            for item in root.findall('.//return'):
                vid_node = item.find('idVeiculo')
                if vid_node is None:
                    continue
                
                try:
                    dt_str = item.find('dataPacote').text or item.find('dataPosicao').text
                    if dt_str is None: continue
                    dt = datetime.fromisoformat(dt_str)

                    # Captura a PLACA se existir
                    placa_node = item.find('placa')
                    placa = placa_node.text if placa_node is not None else None

                    data = {
                        'id_veiculo': int(vid_node.text),
                        'placa': placa,
                        'data_hora': dt,
                        'latitude': float(item.find('latitude').text),
                        'longitude': float(item.find('longitude').text),
                        'odometro': float(item.find('odometro').text),
                        'ignicao': 1 if (item.find('ignicao').text in ('true', '1')) else 0,
                        'velocidade': float(item.find('velocidade').text),
                        'pacote_id': 0
                    }
                    posicoes.append(data)
                except Exception as e:
                    print(f"Erro ao processar item de posição: {e}")
            
            return posicoes
        except Exception as e:
            print(f"Erro ao fazer parse do XML SOAP: {e}")
            if "simultâneas" in xml_content:
                print("AVISO: Limite de consultas simultâneas excedido na Sascar.")
            return []



    def save_positions(self, positions, apply_5min_filter=True):
        """
        Salva posições no banco de dados e mapeia veículos automaticamente.
        Regras:
        - SEMPRE salvar se ignição mudou
        - SEMPRE salvar se passou >=5min desde a última
        - Evitar duplicatas
        """
        if not positions:
            return 0
        
        # *** CORREÇÃO: Ordenar cronologicamente para o filtro funcionar ***
        positions = sorted(positions, key=lambda x: x['data_hora'])
        
        conn = get_connection()
        c = conn.cursor()
        
        # Cache local: {id_veiculo: (data_hora, ignicao)} última salva (inclui batch atual)
        last_saved = {}
        
        count = 0
        for p in positions:
            vid = p['id_veiculo']
            
            # 1. Auto-mapear Placa no banco de veículos
            if p.get('placa'):
                if IS_POSTGRES:
                    c.execute("INSERT INTO veiculos (id_sascar, placa) VALUES (%s, %s) ON CONFLICT (id_sascar) DO UPDATE SET placa=EXCLUDED.placa", 
                              (vid, p['placa']))
                else:
                    c.execute("INSERT OR REPLACE INTO veiculos (id_sascar, placa) VALUES (?, ?)", 
                              (vid, p['placa']))

            # 2. Evitar duplicatas exatas
            c.execute(f'SELECT 1 FROM posicoes_raw WHERE id_veiculo = {get_placeholder(1)} AND data_hora = {get_placeholder(1)}', 
                      (vid, p['data_hora'].isoformat()))
            if c.fetchone():
                continue

            # 3. Verificar última posição (cache local + banco)
            deve_salvar = True
            
            if apply_5min_filter:
                last_dt = None
                last_ignicao = None
                
                # Primeiro, verificar cache local
                if vid in last_saved:
                    last_dt, last_ignicao = last_saved[vid]
                else:
                    # Buscar do banco
                    c.execute(f'''
                        SELECT data_hora, ignicao 
                        FROM posicoes_raw 
                        WHERE id_veiculo = {get_placeholder(1)}
                        ORDER BY data_hora DESC 
                        LIMIT 1
                    ''', (vid,))
                    row = c.fetchone()
                    
                    if row:
                        if isinstance(row[0], str):
                            last_dt = datetime.fromisoformat(row[0].replace('Z', '+00:00'))
                        else:
                            last_dt = row[0]
                        last_ignicao = row[1]
                
                if last_dt is not None:
                    # Normalizar timezone
                    p_dt = p['data_hora']
                    if p_dt.tzinfo is None and last_dt.tzinfo is not None:
                        p_dt = p_dt.replace(tzinfo=last_dt.tzinfo)
                    elif p_dt.tzinfo is not None and last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=p_dt.tzinfo)
                        
                    diff = (p_dt - last_dt).total_seconds()
                    
                    # Regra: Salvar SE passou 5min OU ignição mudou
                    if abs(diff) < 300 and p['ignicao'] == last_ignicao:
                        deve_salvar = False

            # 4. Inserir nova posição (se passou nos filtros)
            if deve_salvar:
                c.execute(f'''
                    INSERT INTO posicoes_raw (id_veiculo, data_hora, latitude, longitude, odometro, ignicao, velocidade, pacote_id)
                    VALUES ({get_placeholder(8)})
                ''', (vid, p['data_hora'].isoformat(), p['latitude'], p['longitude'], p['odometro'], p['ignicao'], p['velocidade'], p['pacote_id']))
                count += 1
                
                # Atualizar cache local
                last_saved[vid] = (p['data_hora'], p['ignicao'])
            
        conn.commit()
        conn.close()
        return count

def format_date_sascar(dt):
    """Formata data para o padrão Sascar: yyyy-MM-dd HH:mm:ss"""
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def obter_ids_veiculos():
    """Retorna lista de IDs de veículo já conhecidos no banco."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT DISTINCT id_sascar FROM veiculos")
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows] if rows else []

if __name__ == "__main__":
    import sys
    import time
    import argparse
    from database import manutencao_banco
    
    parser = argparse.ArgumentParser(description="Sincronizador Sascar")
    parser.add_argument("--hours", type=int, help="Número de horas de histórico para recuperar")
    parser.add_argument("--veiculo", type=int, help="ID específico do veículo para histórico (opcional)")
    args = parser.parse_args()

    # PASSO 1: Manutenção do banco ANTES de começar
    print("=== MANUTENÇÃO DO BANCO DE DADOS ===")
    try:
        manutencao_banco(dias_retencao=30)
    except Exception as e:
        print(f"Erro na manutenção: {e}")
        print("Continuando mesmo assim...")
    
    print("\n" + "="*50 + "\n")
    
    client = SascarClient()
    
    if args.hours:
        # MODO HISTÓRICO
        print(f"=== RECUPERANDO HISTÓRICO (ÚLTIMAS {args.hours} HORAS) ===")
        
        data_fim = datetime.now()
        data_ini = data_fim - timedelta(hours=args.hours)
        
        dataini_str = format_date_sascar(data_ini)
        datafim_str = format_date_sascar(data_fim)
        
        print(f"Período: {dataini_str} até {datafim_str}")
        
        ids_para_sinc = [args.veiculo] if args.veiculo else obter_ids_veiculos()
        
        if not ids_para_sinc:
            print("⚠ Nenhum veículo cadastrado no banco para buscar histórico.")
            print("DICA: Rode o sync normal uma vez para capturar os veículos da fila.")
            sys.exit(0)
            
        print(f"Processando {len(ids_para_sinc)} veículos...")
        
        total_hist_salvas = 0
        for vid in ids_para_sinc:
            print(f" -> Veículo {vid}:")
            
            # Fatiar o período em blocos de 45 minutos (Sascar tem limite de 60min, melhor deixar margem)
            temp_fim = data_fim
            while temp_fim > data_ini:
                temp_ini = max(data_ini, temp_fim - timedelta(minutes=45))
                
                t_ini_str = format_date_sascar(temp_ini)
                t_fim_str = format_date_sascar(temp_fim)
                
                print(f"    - Janela: {t_ini_str} até {t_fim_str}", end=" ", flush=True)
                
                try:
                    xml_hist = client.obter_pacote_posicao_historico(vid, t_ini_str, t_fim_str)
                    
                    if xml_hist:
                        posicoes = client.parse_positions_xml(xml_hist)
                        if posicoes:
                            # Cache de placa
                            conn = get_connection()
                            c = conn.cursor()
                            c.execute(f"SELECT placa FROM veiculos WHERE id_sascar = {get_placeholder(1)}", (vid,))
                            row_placa = c.fetchone()
                            placa_cache = row_placa[0] if row_placa else None
                            conn.close()
                            
                            for p in posicoes:
                                p['id_veiculo'] = vid
                                if not p.get('placa'): p['placa'] = placa_cache
                            
                            salvas = client.save_positions(posicoes)
                            total_hist_salvas += salvas
                            print(f"| ✓ {len(posicoes)} rec, {salvas} salvas.")
                        else:
                            print("| ○ Vazio.")
                    else:
                        print("| ⚠ Falha na resposta.")
                except Exception as e:
                    print(f"| ❌ Erro: {e}")
                
                temp_fim = temp_ini
                time.sleep(0.5) # Pequena pausa entre janelas
            
            time.sleep(1) # Delay entre veículos
            
        print(f"\n=== RESUMO HISTÓRICO ===")
        print(f"Total de novas posições recuperadas: {total_hist_salvas}")

    else:
        # MODO FILA (Comportamento original)
        print("=== DRENANDO FILA DE POSIÇÕES SASCAR ===")
        print("O script vai rodar até esvaziar a fila de posições pendentes.")
        print("Retry automático em caso de falha (não para à madrugada).")
        print("Pressione Ctrl+C para interromper a qualquer momento.\n")
        
        total_recebidas = 0
        total_salvas = 0
        batch_num = 0
        falhas_consecutivas = 0
        MAX_FALHAS = 5
        
        try:
            while True:
                batch_num += 1
                print(f"[Batch {batch_num}] Buscando até 100 posições...")
                
                xml_data = client.obter_pacote_posicoes_com_placa(quantidade=100)
                
                if not xml_data:
                    falhas_consecutivas += 1
                    tempo_espera = min(30, 5 * falhas_consecutivas)  # Backoff exponencial (máx 30s)
                    
                    print(f"⚠ Falha na requisição ({falhas_consecutivas}/{MAX_FALHAS}). Aguardando {tempo_espera}s...")
                    
                    if falhas_consecutivas >= MAX_FALHAS:
                        print("❌ Muitas falhas consecutivas. Encerrando.")
                        break
                    
                    time.sleep(tempo_espera)
                    continue
                
                # Reset contador de falhas se teve sucesso
                falhas_consecutivas = 0
                
                parsed_data = client.parse_positions_xml(xml_data)
                qtd_recebida = len(parsed_data)
                total_recebidas += qtd_recebida
                
                if qtd_recebida == 0:
                    print("✓ Fila vazia! Nenhuma posição pendente.")
                    break
                
                saved_count = client.save_positions(parsed_data)
                total_salvas += saved_count
                
                print(f"  Recebidas: {qtd_recebida} | Salvas: {saved_count} | Total: {total_recebidas} rec / {total_salvas} salvas")
                
                # Pequena pausa para não sobrecarregar a API
                time.sleep(1)
        
        except KeyboardInterrupt:
            print("\n\n⚠ Interrompido pelo usuário.")
        except Exception as e:
            print(f"\n\n❌ Erro inesperado: {e}")
        
        print(f"\n=== RESUMO ===")
        print(f"Total de posições recebidas: {total_recebidas}")
        print(f"Total de posições salvas: {total_salvas}")
        print(f"Taxa de aproveitamento: {(total_salvas/total_recebidas*100) if total_recebidas > 0 else 0:.1f}%")

 
