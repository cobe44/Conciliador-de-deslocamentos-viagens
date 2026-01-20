import sqlite3
import os
import sys

# Tentar carregar variáveis de ambiente localmente
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Tentar carregar secrets do Streamlit Cloud
def get_database_url():
    """Obtém a URL do banco de dados de várias fontes possíveis"""
    # 1. Tentar Streamlit secrets primeiro (para Streamlit Cloud)
    try:
        import streamlit as st
        if hasattr(st, 'secrets'):
            # Tentar DATABASE_URL direto
            if 'DATABASE_URL' in st.secrets:
                return st.secrets['DATABASE_URL']
            # Tentar componentes separados
            if 'database' in st.secrets:
                db = st.secrets['database']
                # Codificar @ na senha como %40
                from urllib.parse import quote_plus
                password = quote_plus(db.get('password', ''))
                return f"postgresql://{db['user']}:{password}@{db['host']}:{db['port']}/{db['database']}"
    except Exception:
        pass
    
    # 2. Variável de ambiente
    return os.getenv("DATABASE_URL")

# Detectar configuração de Banco
DB_URL = get_database_url()
IS_POSTGRES = False

if DB_URL and "postgres" in DB_URL:
    try:
        import psycopg2
        IS_POSTGRES = True
    except ImportError:
        print("⚠️ Driver psycopg2 não encontrado. Para usar PostgreSQL, instale: pip install psycopg2-binary")
        # Fallback ou Exit? Melhor avisar e continuar com SQLite se falhar?
        # User especificou URL, espera Postgres.
        pass


DB_NAME = "frota.db"

def get_connection():
    """Retorna conexão com o banco (SQLite ou PostgreSQL)"""
    if IS_POSTGRES:
        return psycopg2.connect(DB_URL)
    return sqlite3.connect(DB_NAME)

def get_placeholder(count=1):
    """Retorna string de placeholders SQL correta (? ou %s)"""
    token = "%s" if IS_POSTGRES else "?"
    return ", ".join([token] * count)

def init_db():
    conn = get_connection()
    c = conn.cursor()

    # Tipos de dados compatíveis
    if IS_POSTGRES:
        TYPE_PK_AUTO = "SERIAL PRIMARY KEY"
        TYPE_PK_MANUAL = "INTEGER PRIMARY KEY" # Para id_sascar
        TYPE_DATETIME = "TIMESTAMP"
    else:
        TYPE_PK_AUTO = "INTEGER PRIMARY KEY AUTOINCREMENT"
        TYPE_PK_MANUAL = "INTEGER PRIMARY KEY"
        TYPE_DATETIME = "DATETIME"

    # Tabela: Mapeamento de Veículos (ID Sascar é chave, não auto-inc)
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS veiculos (
            id_sascar {TYPE_PK_MANUAL},
            placa TEXT NOT NULL
        )
    ''')

    # Tabela: Pontos de Interesse (POIs)
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS pois (
            id {TYPE_PK_AUTO},
            nome TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            tipo TEXT CHECK(tipo IN ('Base', 'Granja', 'Oficina', 'Concessionaria', 'Posto')) NOT NULL,
            raio INTEGER DEFAULT 100
        )
    ''')

    # Tabela: Dados Brutos de GPS
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS posicoes_raw (
            id {TYPE_PK_AUTO},
            id_veiculo INTEGER NOT NULL,
            data_hora {TYPE_DATETIME} NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            odometro REAL,
            ignicao INTEGER,
            velocidade REAL,
            pacote_id INTEGER
        )
    ''')

    # Tabela: Viagens (Deve vir antes de Deslocamentos por causa da FK)
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS viagens (
            id {TYPE_PK_AUTO},
            placa TEXT NOT NULL,
            data_inicio {TYPE_DATETIME},
            data_fim {TYPE_DATETIME},
            tempo_total REAL DEFAULT 0,
            tempo_parado REAL DEFAULT 0,
            operacao TEXT,
            rota TEXT,
            num_cte TEXT,
            valor REAL DEFAULT 0,
            distancia_total REAL,
            tipo_viagem TEXT,
            observacao TEXT
        )
    ''')

    # Tabela: Deslocamentos (v2 - com campos adicionais para rastreabilidade)
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS deslocamentos (
            id {TYPE_PK_AUTO},
            placa TEXT NOT NULL,
            data_inicio {TYPE_DATETIME} NOT NULL,
            data_fim {TYPE_DATETIME} NOT NULL,
            km_inicial REAL,
            km_final REAL,
            distancia REAL,
            local_inicio TEXT,
            local_fim TEXT,
            tempo REAL DEFAULT 0,
            tempo_ocioso REAL DEFAULT 0,
            situacao TEXT DEFAULT 'MOVIMENTO',
            tipo_parada TEXT DEFAULT 'MOVIMENTO',
            qtd_pontos INTEGER DEFAULT 0,
            raw_id_inicio INTEGER,
            raw_id_fim INTEGER,
            status TEXT DEFAULT 'PENDENTE',
            viagem_id INTEGER,
            FOREIGN KEY(viagem_id) REFERENCES viagens(id)
        )
    ''')

    # Tabela Viagens ja criada acima


    conn.commit()
    
    # Índices (Sintaxe igual para ambos)
    print("Verificando índices...")
    c.execute("CREATE INDEX IF NOT EXISTS idx_posicoes_placa_data ON posicoes_raw(id_veiculo, data_hora)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_deslocamentos_placa_status ON deslocamentos(placa, status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_viagens_placa ON viagens(placa)")
    conn.commit()
    
    # Seeding inicial de POIs
    seed_pois(conn)
    
    conn.close()
    if IS_POSTGRES:
        print("✅ Banco PostgreSQL inicializado.")
    else:
        print("✅ Banco SQLite inicializado.")

def seed_pois(conn):
    c = conn.cursor()
    c.execute("SELECT count(*) FROM pois")
    if c.fetchone()[0] > 0:
        return

    print("Inserindo dados de teste (POIs)...")
    pois_referencia = [
        ("Base SP", -23.550520, -46.633308, "Base", 200),
        ("Granja Modelo", -23.555520, -46.638308, "Granja", 500),
        ("Oficina Central", -23.560000, -46.640000, "Oficina", 300),
        ("Posto Descanso", -23.570000, -46.650000, "Posto", 300)
    ]
    
    ph = get_placeholder(5) # ?,?,?,?,? ou %s,%s,%s,%s,%s
    sql = f"INSERT INTO pois (nome, latitude, longitude, tipo, raio) VALUES ({ph})"
    
    c.executemany(sql, pois_referencia)
    conn.commit()

def manutencao_banco(dias_retencao=30):
    from datetime import datetime, timedelta
    
    conn = get_connection()
    c = conn.cursor()
    
    data_limite = datetime.now() - timedelta(days=dias_retencao)
    data_limite_str = data_limite.isoformat()
    
    print(f"[MANUTENÇÃO] Limpeza anterior a {data_limite.strftime('%d/%m/%Y')}")
    
    ph = get_placeholder(1)
    c.execute(f"SELECT COUNT(*) FROM posicoes_raw WHERE data_hora < {ph}", (data_limite_str,))
    registros_antigos = c.fetchone()[0]
    
    if registros_antigos > 0:
        c.execute(f"DELETE FROM posicoes_raw WHERE data_hora < {ph}", (data_limite_str,))
        conn.commit()
        # VACUUM (funciona no PG também para reclaim space, mas cuidado com lock)
        # Em PG, autovacuum faz isso. Em SQLite é manual.
        # Vamos manter apenas para SQLite.
        if not IS_POSTGRES:
            c.execute("VACUUM")
        print(f"[MANUTENÇÃO] {registros_antigos} registros removidos.")
    
    conn.close()

def execute_insert_returning_id(cursor, sql, params):
    """Executa INSERT e retorna o ID do novo registro, compatível com PG/SQLite"""
    if IS_POSTGRES:
        if "RETURNING" not in sql.upper():
            sql += " RETURNING id"
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return row[0] if row else None
    else:
        cursor.execute(sql, params)
        return cursor.lastrowid

def get_pois():
    """Retorna todos os POIs do banco de dados"""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT id, nome, latitude, longitude, tipo, raio FROM pois")
    rows = c.fetchall()
    conn.close()
    return rows

def migrate_db():
    """
    Migração segura: Adiciona novas colunas se não existirem.
    Pode ser executado várias vezes sem problemas.
    """
    conn = get_connection()
    c = conn.cursor()
    
    # Lista de migrações: (nome_coluna, definição_sql)
    migrations = [
        ("tipo_parada", "TEXT DEFAULT 'MOVIMENTO'"),
        ("qtd_pontos", "INTEGER DEFAULT 0"),
        ("raw_id_inicio", "INTEGER"),
        ("raw_id_fim", "INTEGER"),
    ]
    
    for col_name, col_def in migrations:
        try:
            if IS_POSTGRES:
                c.execute(f"ALTER TABLE deslocamentos ADD COLUMN IF NOT EXISTS {col_name} {col_def}")
            else:
                # SQLite não tem IF NOT EXISTS para ADD COLUMN
                c.execute(f"ALTER TABLE deslocamentos ADD COLUMN {col_name} {col_def}")
        except Exception as e:
            # Coluna já existe (SQLite lança erro)
            if "duplicate column" not in str(e).lower() and "already exists" not in str(e).lower():
                print(f"⚠️ Migração {col_name}: {e}")
    
    conn.commit()
    conn.close()
    print("✅ Migração do banco concluída.")

if __name__ == "__main__":
    init_db()
    migrate_db()
