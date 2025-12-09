# src/main.py
from src.config import DB
from src.ingest import read_csv, rows_from_df
from src.normalize import normalize_row
from src.db import get_conn, insert_medicion
import sys
from pathlib import Path
import logging

logging.basicConfig(filename='run_log.txt', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def run_batch(csv_path):
    p = Path(csv_path)
    if not p.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {p.resolve()}")
    df = read_csv(str(p))
    conn = get_conn(DB)
    for raw in rows_from_df(df):
        m = normalize_row(raw)
        insert_medicion(conn, m, procedure_version='v1')
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python -m src.main ruta_a_csv")
        print("Ejemplo: python -m src.main data/ejemplo.csv")
        sys.exit(1)
    run_batch(sys.argv[1])
    print("Proceso finalizado correctamente.")
