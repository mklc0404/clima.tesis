# src/db.py (reemplazar insert_medicion con esta versión)
import psycopg2
from psycopg2.extras import Json
import traceback

def get_conn(config):
    return psycopg2.connect(
        host=config['host'],
        dbname=config['dbname'],
        user=config['user'],
        password=config['password'],
        port=config['port']
    )

def ensure_sensor_exists(cur, sensor_id):
    # Inserta un sensor mínimo si no existe
    cur.execute("SELECT 1 FROM sensor WHERE sensor_id = %s", (sensor_id,))
    if cur.fetchone() is None:
        cur.execute(
            "INSERT INTO sensor (sensor_id, nombre, modelo, fabricante) VALUES (%s,%s,%s,%s) ON CONFLICT (sensor_id) DO NOTHING",
            (sensor_id, f"Sensor {sensor_id}", None, None)
        )

def insert_medicion(conn, m, procedure_version='v1'):
    """
    m: dict con keys:
      sensor_id, timestamp, temperatura, humedad, presion,
      radiacion_solar, velocidad_viento, raw (dict), validation_flags (list)
    """
    with conn.cursor() as cur:
        try:
            # 1) asegurar existencia del sensor (para evitar FK violation)
            sensor_id = m.get('sensor_id') or 'unknown'
            ensure_sensor_exists(cur, sensor_id)

            CONSISTENCY_THRESHOLDS = {
                    'temperatura': 10.0,   # degC
                    'humedad': 30.0,       # %
                    'presion': 50.0        # hPa
                }

            cur.execute("SELECT temperatura, humedad, presion, timestamp FROM medicion WHERE sensor_id=%s ORDER BY timestamp DESC LIMIT 1", (sensor_id,))
            last = cur.fetchone()
            if last and last[3] is not None:
                last_temp, last_hum, last_pres, last_ts = last[0], last[1], last[2], last[3]
                # comparar y si excede umbral añadir flag a m['validation_flags']
                if last_temp is not None and m.get('temperatura') is not None:
                    if abs(m['temperatura'] - last_temp) > CONSISTENCY_THRESHOLDS['temperatura']:
                        m.setdefault('validation_flags', []).append({'tipo':'CONSISTENCY','descripcion': f'temp change {m["temperatura"]} vs {last_temp}'})
                if last_hum is not None and m.get('humedad') is not None:
                    if abs(m['humedad'] - last_hum) > CONSISTENCY_THRESHOLDS['humedad']:
                        m.setdefault('validation_flags', []).append({'tipo':'CONSISTENCY','descripcion': f'hum change {m["humedad"]} vs {last_hum}'})

            # 2) insertar medición
            cur.execute(
                """
                
                INSERT INTO medicion
                (sensor_id, timestamp, temperatura, humedad, presion, radiacion_solar, velocidad_viento, procedure_version, raw_payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (sensor_id, timestamp) DO UPDATE
                  SET temperatura = EXCLUDED.temperatura,
                      humedad = EXCLUDED.humedad,
                      presion = EXCLUDED.presion,
                      radiacion_solar = EXCLUDED.radiacion_solar,
                      velocidad_viento = EXCLUDED.velocidad_viento,
                      ingest_ts = now(),
                      raw_payload = EXCLUDED.raw_payload;
                """,
                (
                    sensor_id,
                    m.get('timestamp'),
                    m.get('temperatura'),
                    m.get('humedad'),
                    m.get('presion'),
                    m.get('radiacion_solar'),
                    m.get('velocidad_viento'),
                    procedure_version,
                    Json(m.get('raw', {}))
                )
            )
            
            # 3) insertar validaciones si las hay
            cur.execute(
                "SELECT medicion_id FROM medicion WHERE sensor_id=%s AND timestamp=%s",
                (sensor_id, m.get('timestamp'))
            )
            row = cur.fetchone()
            medicion_id = row[0] if row else None

            for vf in m.get('validation_flags', []):
                cur.execute(
                    "INSERT INTO validacion (medicion_id, tipo_flag, descripcion_problema) VALUES (%s,%s,%s)",
                    (medicion_id, vf.get('tipo'), vf.get('descripcion'))
                )

        except Exception as e:
            # imprime error detallado (útil para depuración)
            import traceback
            print("ERROR en insert_medicion:", e)
            traceback.print_exc()
            raise
    conn.commit()
