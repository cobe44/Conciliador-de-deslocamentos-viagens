"""
LOGLIVE Processor - V5 (State Machine)
=======================================
GPS → Events with IBGE geocoding.

State Machine Logic:
1. Start in DESLOCAMENTO mode
2. Accumulate movement until vel < 3 km/h for > 15 min consecutively
3. Switch to PARADA mode
4. Accumulate parada until movement covers > 3 km
5. Switch back to DESLOCAMENTO mode
6. Events < 5 min are filtered out
"""

import pandas as pd
import uuid
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# Configuration
STOP_TIME_THRESHOLD = 15  # minutes - stop for this long = PARADA
MOVE_KM_THRESHOLD = 3     # km - move this far = new DESLOCAMENTO
VELOCITY_STOP = 3         # km/h
MIN_EVENT_DURATION = 5    # minutes - filter short events

from database import DATABASE_URL
engine = create_engine(DATABASE_URL)

try:
    from services.ibge_geocoding import get_municipio_ibge
    HAS_IBGE = True
except ImportError:
    HAS_IBGE = False


def get_city(lat, lon):
    if HAS_IBGE:
        return get_municipio_ibge(lat, lon)
    return f"({lat:.4f}, {lon:.4f})"


def ensure_tables():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS deslocamentos (
                id VARCHAR(100) PRIMARY KEY, placa VARCHAR(20) NOT NULL, truck_id INTEGER NOT NULL,
                tipo VARCHAR(20) NOT NULL, data_inicio TIMESTAMP NOT NULL, data_fim TIMESTAMP NOT NULL,
                duracao_min FLOAT DEFAULT 0, dist_km FLOAT DEFAULT 0, motor_ligado_min FLOAT DEFAULT 0,
                lat_inicio FLOAT, lon_inicio FLOAT, lat_fim FLOAT, lon_fim FLOAT,
                local_inicio VARCHAR(100), local_fim VARCHAR(100),
                validado BOOLEAN DEFAULT FALSE, trip_id VARCHAR(100), 
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS final_trips (
                id VARCHAR(100) PRIMARY KEY, placa VARCHAR(20), truck_id INTEGER,
                data_inicio TIMESTAMP, data_fim TIMESTAMP, origem VARCHAR(100), destino VARCHAR(100),
                km_total FLOAT DEFAULT 0, tempo_mov_min FLOAT DEFAULT 0, tempo_par_min FLOAT DEFAULT 0,
                motorista VARCHAR(100), cte VARCHAR(50), valor FLOAT DEFAULT 0,
                tipo VARCHAR(50) DEFAULT 'Produtiva', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.commit()
    print("✅ Tables OK")


def get_plates():
    return pd.read_sql("SELECT DISTINCT placa, id_sascar FROM veiculos WHERE placa IS NOT NULL ORDER BY placa", engine).to_dict('records')


def process_plate(placa: str, truck_id: int, start_date: datetime = None, end_date: datetime = None):
    if end_date is None:
        end_date = datetime.now()
    if start_date is None:
        start_date = end_date - timedelta(days=7)
    
    print(f"  📍 {placa}...", end=" ", flush=True)
    
    # Clear old
    with engine.connect() as conn:
        conn.execute(text(f"DELETE FROM deslocamentos WHERE placa = '{placa}' AND validado = FALSE"))
        conn.commit()
    
    # Fetch GPS
    q = f"""
        SELECT id, id_veiculo, data_hora, latitude, longitude, velocidade, ignicao, odometro
        FROM posicoes_raw WHERE id_veiculo = {truck_id}
        AND data_hora BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY data_hora
    """
    df = pd.read_sql(q, engine)
    
    if df.empty or len(df) < 2:
        print("no data")
        return 0
    
    df['data_hora'] = pd.to_datetime(df['data_hora'])
    df = df.sort_values('data_hora').reset_index(drop=True)
    
    # State machine
    events = []
    current_state = 'DESLOCAMENTO'  # Start moving
    event_start_idx = 0
    stop_start_idx = None
    stop_start_time = None
    move_start_odo = None
    
    for i in range(len(df)):
        row = df.iloc[i]
        is_stopped = row['velocidade'] < VELOCITY_STOP
        
        if current_state == 'DESLOCAMENTO':
            # In movement mode - look for long stop
            if is_stopped:
                if stop_start_idx is None:
                    stop_start_idx = i
                    stop_start_time = row['data_hora']
                else:
                    # Check if stopped for > threshold
                    stop_duration = (row['data_hora'] - stop_start_time).total_seconds() / 60
                    if stop_duration >= STOP_TIME_THRESHOLD:
                        # End current DESLOCAMENTO at stop_start_idx - 1
                        if stop_start_idx > event_start_idx:
                            end_idx = stop_start_idx - 1
                            events.append({
                                'state': 'DESLOCAMENTO',
                                'start_idx': event_start_idx,
                                'end_idx': end_idx
                            })
                        # Start PARADA at stop_start_idx
                        current_state = 'PARADA'
                        event_start_idx = stop_start_idx
                        stop_start_idx = None
                        stop_start_time = None
                        move_start_odo = None
            else:
                # Moving - reset stop counter
                stop_start_idx = None
                stop_start_time = None
        
        else:  # PARADA
            # In stop mode - look for significant movement (> 3 km)
            if not is_stopped:
                if move_start_odo is None:
                    move_start_odo = row['odometro']
                else:
                    # Check if moved > threshold km
                    distance = row['odometro'] - move_start_odo
                    if distance >= MOVE_KM_THRESHOLD:
                        # End current PARADA at previous point
                        end_idx = i - 1
                        if end_idx >= event_start_idx:
                            events.append({
                                'state': 'PARADA',
                                'start_idx': event_start_idx,
                                'end_idx': end_idx
                            })
                        # Start new DESLOCAMENTO
                        current_state = 'DESLOCAMENTO'
                        event_start_idx = i
                        move_start_odo = None
                        stop_start_idx = None
                        stop_start_time = None
            else:
                # Stopped again - reset move counter
                move_start_odo = None
    
    # Close last event
    if len(df) > event_start_idx:
        events.append({
            'state': current_state,
            'start_idx': event_start_idx,
            'end_idx': len(df) - 1
        })
    
    if not events:
        print("no events")
        return 0
    
    # Convert to insertable format
    count = 0
    with engine.connect() as conn:
        for evt in events:
            start_idx = evt['start_idx']
            end_idx = evt['end_idx']
            
            start_row = df.iloc[start_idx]
            end_row = df.iloc[end_idx]
            
            t0 = start_row['data_hora']
            t1 = end_row['data_hora']
            dur = (t1 - t0).total_seconds() / 60
            
            # Skip short events
            if dur < MIN_EVENT_DURATION:
                continue
            
            lat_i, lon_i = start_row['latitude'], start_row['longitude']
            lat_f, lon_f = end_row['latitude'], end_row['longitude']
            
            if evt['state'] == 'PARADA':
                dist = 0
                # Motor time = sum of time when ignition on
                subset = df.iloc[start_idx:end_idx+1]
                motor = subset['ignicao'].sum() * 5 / 60  # Approximate: each point ~5 min apart
                
                # For PARADA, use median coordinates to avoid GPS glitches
                lat_i = lat_f = subset['latitude'].median()
                lon_i = lon_f = subset['longitude'].median()
            else:
                dist = max(0, end_row['odometro'] - start_row['odometro'])
                motor = 0
            
            eid = str(uuid.uuid4())
            local_i = str(get_city(lat_i, lon_i)).replace("'", "''")[:100]
            local_f = str(get_city(lat_f, lon_f)).replace("'", "''")[:100]
            
            sql = f"""
                INSERT INTO deslocamentos (id, placa, truck_id, tipo, data_inicio, data_fim, 
                    duracao_min, dist_km, motor_ligado_min, 
                    lat_inicio, lon_inicio, lat_fim, lon_fim, local_inicio, local_fim)
                VALUES ('{eid}', '{placa}', {truck_id}, '{evt['state']}', 
                    '{t0}', '{t1}', {round(dur, 2)}, {round(dist, 2)}, 
                    {round(motor, 2)}, {lat_i}, {lon_i}, {lat_f}, {lon_f}, '{local_i}', '{local_f}')
            """
            conn.execute(text(sql))
            count += 1
        
        conn.commit()
    
    print(f"{count} events")
    return count


from concurrent.futures import ThreadPoolExecutor, as_completed

def process_parallel(plates, start_fn, end_fn):
    """
    Helper to process plates in parallel
    start_fn: function(plate_record) -> start_date
    end_fn: function(plate_record) -> end_date
    """
    total_events = 0
    max_workers = 5  # Limit concurrency to avoid DB overload
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_plate = {}
        for p in plates:
            try:
                start = start_fn(p)
                end = end_fn(p)
                # Only submit if we have valid dates
                if start and end:
                    future = executor.submit(process_plate, p['placa'], p['id_sascar'], start, end)
                    future_to_plate[future] = p['placa']
            except Exception as e:
                print(f"❌ Error preparing {p['placa']}: {e}")
        
        for future in as_completed(future_to_plate):
            placa = future_to_plate[future]
            try:
                count = future.result()
                total_events += count
            except Exception as e:
                print(f"❌ {placa}: {e}")
                
    return total_events


def reprocessar(dias: int = 7):
    print(f"🔄 LOGLIVE Processor V5 (State Machine) - Últimos {dias} dias")
    print(f"⚡ Parallel Execution (Max 5 workers)")
    ensure_tables()
    
    plates = get_plates()
    print(f"📋 {len(plates)} veículos")
    
    base_end = datetime.now()
    base_start = base_end - timedelta(days=dias)
    
    # Define time range functions
    get_start = lambda p: base_start
    get_end = lambda p: base_end
    
    total = process_parallel(plates, get_start, get_end)
    
    print(f"\n✅ {total} eventos")
    return total


def incremental():
    """
    Incremental processing - only process new GPS data since last processed event.
    Much more efficient for scheduled jobs.
    """
    print("🔄 LOGLIVE Processor V5 - Modo Incremental")
    print(f"⚡ Parallel Execution (Max 5 workers)")
    ensure_tables()
    
    plates = get_plates()
    print(f"📋 {len(plates)} veículos")
    
    def get_incremental_start(p):
        placa = p['placa']
        truck_id = p['id_sascar']
        
        # Get last processed timestamp for this plate
        # We need a fresh engine connection for thread safety if using specific drivers, 
        # but SQLAlchemy engine is thread-safe.
        try:
             last_event = pd.read_sql(
                f"SELECT MAX(data_fim) as last_time FROM deslocamentos WHERE placa = '{placa}'", 
                engine
            )
        except:
             return None

        if last_event.iloc[0]['last_time'] is not None:
            # Start from last event minus 1 hour overlap (to catch edge cases)
            start = pd.to_datetime(last_event.iloc[0]['last_time']) - timedelta(hours=1)
        else:
            # No events yet - process last 24 hours
            start = datetime.now() - timedelta(days=1)
            
        # Optimization: Check if there's new data before submitting task
        # This prevents spinning up threads for vehicles with no new data
        check = pd.read_sql(
            f"SELECT COUNT(*) as cnt FROM posicoes_raw WHERE id_veiculo = {truck_id} AND data_hora > '{start}'",
            engine
        )
        
        if check.iloc[0]['cnt'] > 0:
            return start
        else:
            return None # Skip this vehicle

    def get_incremental_end(p):
        return datetime.now()

    total = process_parallel(plates, get_incremental_start, get_incremental_end)
    
    print(f"\n✅ {total} eventos processados")
    return total


def process_single(placa: str, dias: int = 7):
    ensure_tables()
    df = pd.read_sql(f"SELECT id_sascar FROM veiculos WHERE placa = '{placa}' LIMIT 1", engine)
    if df.empty:
        print(f"❌ Placa não encontrada: {placa}")
        return 0
    
    return process_plate(placa, int(df.iloc[0]['id_sascar']), 
                         datetime.now() - timedelta(days=dias), datetime.now())


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == '--reprocessar':
            reprocessar(int(sys.argv[2]) if len(sys.argv) > 2 else 7)
        elif sys.argv[1] == '--incremental':
            incremental()
        elif sys.argv[1] == '--placa' and len(sys.argv) > 2:
            process_single(sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 7)
        else:
            print("python processor.py --reprocessar [dias]")
            print("python processor.py --incremental")
            print("python processor.py --placa PLACA [dias]")
    else:
        print("LOGLIVE Processor V5")
        print("  --reprocessar [dias]  # Reprocessa todos os dados")
        print("  --incremental         # Processa apenas dados novos (para cron)")
        print("  --placa PLACA [dias]  # Processa uma placa específica")
