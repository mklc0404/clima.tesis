# debug_pipeline.py
# Ejecuta: python debug_pipeline.py
# Muestra paso a paso la configuración, conexión, esquema y prueba de inserción.

import traceback
import json
import sys
from pathlib import Path

def safe_print(title, obj):
    print("\n" + "="*10 + " " + title + " " + "="*10)
    try:
        print(obj)
    except Exception:
        print(repr(obj))

def main():
    print("DEBUG START")
    try:
        # 1) verificar config desde src.config
        try:
            from src.config import DB
            safe_print("DB config (src.config.DB)", DB)
        except Exception as e:
            print("ERROR: no pude importar src.config.DB ->", e)
            traceback.print_exc()
            DB = None

        # 2) conectar a la DB con psycopg2 y mostrar current_database y tablas
        try:
            import psycopg2
            from psycopg2.extras import Json
            conn = psycopg2.connect(
                host=DB['host'],
                dbname=DB['dbname'],
                user=DB['user'],
                password=DB['password'],
                port=DB['port']
            )
            conn.autocommit = True
            safe_print("Conexión DSN", conn.dsn if hasattr(conn,'dsn') else conn)
        except Exception as e:
            print("ERROR: no pude conectar con psycopg2 usando src.config.DB ->", e)
            traceback.print_exc()
            return

        cur = conn.cursor()
        try:
            cur.execute("SELECT current_database();")
            safe_print("current_database()", cur.fetchone())
        except Exception as e:
            print("ERROR leyendo current_database:", e)
            traceback.print_exc()

        # listar tablas públicas
        try:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema='public' ORDER BY table_name;
            """)
            tables = [r[0] for r in cur.fetchall()]
            safe_print("Tablas en public", tables)
        except Exception as e:
            print("ERROR listando tablas:", e)
            traceback.print_exc()

        # mostrar esquema de medicion si existe
        try:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name='medicion'
                ORDER BY ordinal_position;
            """)
            cols = cur.fetchall()
            safe_print("Esquema de tabla 'medicion' (column_name, data_type)", cols)
        except Exception as e:
            print("Tabla 'medicion' no encontrada o error al obtener esquema:", e)
            traceback.print_exc()

        # contar filas
        try:
            cur.execute("SELECT COUNT(*) FROM medicion;")
            cnt = cur.fetchone()[0]
            safe_print("COUNT(*) FROM medicion", cnt)
        except Exception as e:
            print("No se pudo contar filas (quizás tabla no existe):", e)
            traceback.print_exc()

        # 3) mostrar primeras filas (si hay)
        try:
            cur.execute("SELECT * FROM medicion LIMIT 5;")
            rows = cur.fetchall()
            safe_print("Primeras filas medicion (raw)", rows)
        except Exception as e:
            print("No se pudieron obtener filas (y/o tabla vacía):", e)
            traceback.print_exc()

        # 4) ejecutar normalización en el CSV y mostrar resultados (no insertar)
        try:
            import pandas as pd
            from src.normalize import normalize_row
            csv_path = Path("data/ejemplo.csv")
            if not csv_path.exists():
                print("Archivo data/ejemplo.csv NO existe en la ruta esperada:", csv_path.resolve())
            else:
                df = pd.read_csv(str(csv_path))
                safe_print("Filas CSV (count)", len(df))
                for i, r in df.iterrows():
                    raw = r.to_dict()
                    norm = normalize_row(raw)
                    safe_print(f"NORMALIZED row {i}", norm)
        except Exception as e:
            print("ERROR ejecutando normalize_row:", e)
            traceback.print_exc()

        # 5) Intento manual de inserción de 1 fila de prueba (para verificar permisos y esquema)
        try:
            # Construir fila de prueba similar a normalize_row output
            test_row = {
                'sensor_id': 'dbg_s1',
                'timestamp': None,
                'temperatura': 25.123,
                'humedad': 55.0,
                'presion': 1005.5,
                'radiacion_solar': None,
                'velocidad_viento': None,
                'raw': {'test': True, 'source': 'debug'},
                'validation_flags': []
            }

            # Si csv contenía filas usa su timestamp
            try:
                if 'df' in locals() and len(df)>0:
                    import pandas as pd
                    ts = pd.to_datetime(df.iloc[0].get('time') or df.iloc[0].get('timestamp') or df.iloc[0].get('date'))
                    test_row['timestamp'] = ts.to_pydatetime()
                else:
                    import datetime
                    test_row['timestamp'] = datetime.datetime.utcnow()
            except Exception as e:
                import datetime
                test_row['timestamp'] = datetime.datetime.utcnow()

            safe_print("Test row to insert", test_row)

            # Inserción manual
            try:
                cur.execute("""
                    INSERT INTO medicion
                    (sensor_id, timestamp, temperatura, humedad, presion, radiacion_solar, velocidad_viento, procedure_version, raw_payload)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING medicion_id;
                """, (
                    test_row['sensor_id'],
                    test_row['timestamp'],
                    test_row['temperatura'],
                    test_row['humedad'],
                    test_row['presion'],
                    test_row['radiacion_solar'],
                    test_row['velocidad_viento'],
                    'debug',
                    Json(test_row['raw']) if 'Json' in globals() else psycopg2.extras.Json(test_row['raw'])
                ))
                new_id = cur.fetchone()[0]
                safe_print("INSERT OK -> medicion_id", new_id)
            except Exception as e:
                print("ERROR al insertar fila de prueba:", e)
                traceback.print_exc()

            # contar despues
            try:
                cur.execute("SELECT COUNT(*) FROM medicion;")
                cnt2 = cur.fetchone()[0]
                safe_print("COUNT(*) FROM medicion (after insert)", cnt2)
            except Exception as e:
                print("ERROR contando despues de insert:", e)
                traceback.print_exc()

        except Exception as e:
            print("ERROR en bloque de inserción de prueba:", e)
            traceback.print_exc()

        # 6) cerrar
        try:
            cur.close()
            conn.close()
            print("\nDEBUG END")
        except:
            pass

    except Exception as e:
        print("ERROR inesperado en debug_pipeline:", e)
        traceback.print_exc()

if __name__ == "__main__":
    main()
