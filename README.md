# Conciliador de Fretes 

Sistema robusto de gestão de telemetria para 30 caminhões com integração Sascar e processamento automático de viagens.

## 🏗 Arquitetura Implementada

### 1. Base de Dados (`database.py`)
- **SQLite** com otimização automática (índices em colunas críticas)
- **Manutenção Automática**: Remove dados antigos (>30 dias) e executa VACUUM
- **Tabelas**:
  - `veiculos` - Mapeamento ID Sascar ↔ Placa
  - `pois` - Pontos de Interesse (geofencing)
  - `posicoes_raw` - Telemetria bruta (com retenção de 30 dias)
  - `viagens` - Histórico permanente de viagens processadas

### 2. Importação de POIs (`import_pois.py`)
```powershell
python import_pois.py meus_pontos.xlsx
```
Importa ficheiro Excel/CSV com colunas: `Nome, Latitude, Longitude, Raio, Tipo`

### 3. Sincronização Inteligente (`sascar_sync.py`)
**Funcionalidades Implementadas:**
- ✅ Drenagem automática da fila até esvaziar
- ✅ Filtro de 5 minutos (evita inchaço do banco)
- ✅ **Exceção**: SEMPRE salva se ignição mudou (captura paradas/arranques)
- ✅ Retry automático com backoff exponencial (não para à madrugada)
- ✅ Captura automática de placas da API

**Regras de Salvamento:**
1. Salvar SE passou ≥5min desde última posição
2. Salvar SE ignição mudou (mesmo <5min)
3. Evitar duplicatas exatas

**Uso:**
```powershell
python sascar_sync.py
```

### 4. Processamento (`processor.py`)
- Geofencing com Haversine
- Classificação automática:
  - **PRODUTIVA**: Base → Granja → Base
  - **APOIO**: Granja A → Granja B
  - **MANUTENÇÃO**: Destino = Oficina/Concessionária
- Cálculo de KM via odómetro

### 5. Dashboard (`app.py`)
Interface Streamlit com:
- Filtros por Placa e Data
- Mapa interativo (Polyline das rotas)
- Exportação para Excel
- Gráficos de ociosidade

## 📦 Instalação

```powershell
# 1. Criar ambiente virtual
python -m venv venv
.\venv\Scripts\activate

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Configurar credenciais
# Editar .env com suas credenciais Sascar
```

## 🚀 Uso Diário

### Primeiro Uso (Setup)
```powershell
# 1. Inicializar banco
python database.py

# 2. Importar POIs (se tiver ficheiro Excel)
python import_pois.py meus_pontos.xlsx

# 3. OU usar o seeder automático
python poi_seeder.py

# 4. Primeira sincronização (drena a fila completa)
python sascar_sync.py
```

### Rotina Automática e Automação
```powershell
# Executar a cada 15-30 min (via Agendador de Tarefas do Windows)
python sascar_sync.py  
```

> [!TIP]
> **Automação no Windows**: 
> 1. Abra o "Agendador de Tarefas".
> 2. Crie uma Tarefa Básica: "Sync Sascar".
> 3. Disparador: Diariamente (repita a cada 30 minutos).
> 4. Ação: "Iniciar um programa".
> 5. Programa: `powershell.exe`
> 6. Argumentos: `-ExecutionPolicy Bypass -Command "& 'C:\caminho\para\projeto\venv\Scripts\python.exe' 'C:\caminho\para\projeto\sascar_sync.py'"`

### Recuperação de Dados (Gaps)
Se o sistema ficou desligado por muito tempo e você percebeu "buracos" no dashboard, use o modo histórico:

```powershell
# Recuperar as últimas 24 horas de todos os veículos
python sascar_sync.py --hours 24

# Recuperar as últimas 12 horas de um veículo específico (ID Sascar)
python sascar_sync.py --hours 12 --veiculo 12345
```

### Processamento de Viagens
```powershell
python processor.py    # Processa viagens com base nas novas posições
```

### Visualização
```powershell
streamlit run app.py
```
Acessar: `http://localhost:8501`

## 🛠 Funcionalidades Avançadas

### Manutenção Manual do Banco
```python
from database import manutencao_banco
manutencao_banco(dias_retencao=30)  # Limpa dados >30 dias
```

### Importar POIs de Excel
Formato do ficheiro:
| Nome | Latitude | Longitude | Raio | Tipo |
|------|----------|-----------|------|------|
| Base Principal | -23.550520 | -46.633308 | 3000 | Base |
| Granja A | -23.555520 | -46.638308 | 600 | Granja |

Tipos válidos: `Base`, `Granja`, `Oficina`, `Concessionaria`, `Posto`

## 📊 Estrutura de Diretórios

```
conciliador_fretes_v2/
├── database.py          # Gestão SQLite + manutenção
├── import_pois.py       # Importador de POIs via Excel
├── poi_data.py          # Dados hardcoded de POIs reais
├── poi_seeder.py        # Popular banco com POIs de poi_data.py
├── sascar_sync.py       # Sincronização com retry
├── processor.py         # Motor de processamento
├── app.py               # Dashboard Streamlit
├── billing_import.py    # Importação de faturas XML
├── frota.db             # Banco SQLite (gerado automaticamente)
├── .env                 # Credenciais (não versionar!)
└── requirements.txt
```

## ⚙ Configurações

### Variáveis de Ambiente (`.env`)
```env
SASCAR_USER=seu_usuario
SASCAR_PASS=sua_senha
```

### Parâmetros Ajustáveis

**`sascar_sync.py`:**
- `dias_retencao`: Tempo de retenção de posições (padrão: 30 dias)
- `MAX_FALHAS`: Falhas consecutivas antes de desistir (padrão: 5)

**`processor.py`:**
- Raios de POIs (configurável via banco de dados)
- Timeout de viagem (atualmente fixo em 30min)

## 🔧 Resolução de Problemas

### Erro HTTP 500 da Sascar
- **Causa**: Limite de consultas simultâneas ou dados inválidos
- **Solução**: O retry automático vai lidar. Se persistir, verificar credenciais.

### Banco de dados muito grande
```powershell
# Executar manutenção manual
python -c "from database import manutencao_banco; manutencao_banco(15)"
```

### POIs não detetados
- Verificar raios configurados no banco
- Usar raio maior para testes: UPDATE pois SET raio=600 WHERE tipo='Granja'

## 📝 Notas Técnicas

- **Filtro de 5min**: Mantém o banco compacto (espera-se ~288 posições/dia/veículo para 30 camiões = ~260k registros/mês antes da limpeza)
- **VACUUM automático**: Recupera espaço após DELETE (executado pela `manutencao_banco()`)
- **Histórico de viagens**: NUNCA é apagado (tabela `viagens` é permanente)
- **Performance**: Índices criados automaticamente em `(id_veiculo, data_hora)` para queries rápidas

## 🎯 Próximos Passos

- [ ] Scheduler automático (Windows Task ou cron)
- [ ] Alertas de ociosidade via email
- [ ] Reconciliação XML vs GPS (módulo `billing_import.py`)
- [ ] API REST para integração externa

---


## ☁️ Deploy na Nuvem (Streamlit Cloud + Supabase)

1. **GitHub**:
   - Crie um repositório privado no GitHub.
   - Faça upload de todos os arquivos (exceto `.env`, `frota.db` e pastas `venv/__pycache__`).
   - O arquivo `.gitignore` criado já previne o envio de arquivos sensíveis.

2. **Streamlit Community Cloud**:
   - Conecte seu GitHub e selecione o repositório.
   - Em **Advanced Settings** -> **Secrets**, adicione:
     ```toml
     SASCAR_USER = "seu_usuario"
     SASCAR_PASS = "sua_senha"
     SASCAR_WSDL = "https://sasintegra.sascar.com.br/SasIntegra/SasIntegraWSService?wsdl"
     DATABASE_URL = "postgresql://postgres......@aws-1....supabase.com:6543/postgres"
     ```

3. **Popular o Banco na Nuvem**:
   - Como o banco começa vazio, rode localmente os scripts apontando para a nuvem (com o `.env` configurado):
     ```bash
     python poi_seeder.py   # Popula os POIs reais
     python sascar_sync.py  # Busca veículos e posições da Sascar
     ```

---
**Desenvolvido para gestão eficiente de frota avícola** 🚛🐔

