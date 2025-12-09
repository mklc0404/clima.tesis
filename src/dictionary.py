from src.config import DB
import psycopg2

def load_variable_map():
    """
    Retorna dict: { sinonimo -> nombre_estandar }
    """
    conn = psycopg2.connect(
        host=DB['host'], dbname=DB['dbname'], user=DB['user'],
        password=DB['password'], port=DB['port']
    )
    cur = conn.cursor()
    cur.execute("SELECT nombre_sinonimo, nombre_estandar FROM variable_sinonimo;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {r[0].lower(): r[1] for r in rows}