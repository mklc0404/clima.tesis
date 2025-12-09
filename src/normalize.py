# src/normalize.py
from datetime import timezone
import pandas as pd
from .utils import f_to_c, pa_to_hpa, decimal_to_percent
import math

# intento cargar diccionario desde DB (opcional)
try:
    from .dictionary import load_variable_map
    VAR_MAP_DB = load_variable_map()  # e.g. {'temp_f':'temperatura', 'p':'presion'}
except Exception:
    VAR_MAP_DB = {}

# fallback local mapping (como ya tenías)
LOCAL_MAP = {
    'temp_f': ('temperatura','f'),
    'temp': ('temperatura','c'),
    't': ('temperatura','c'),
    'temperature': ('temperatura','c'),
    'h': ('humedad','percent'),
    'hum': ('humedad','percent'),
    'humidity': ('humedad','percent'),
    'p': ('presion','pa'),
    'pa': ('presion','pa'),
    'pressure': ('presion','pa'),
    # ...
}

def resolve_variable(key):
    k = key.lower()
    if k in VAR_MAP_DB:
        # DB only gives nombre_estandar; try to infer hint from key (pa vs f)
        est = VAR_MAP_DB[k]
        # heurística: if key contains 'f' -> temp f, if 'pa' or key=='p' -> pa
        if 'f' in k:
            return est, 'f'
        if 'pa' in k or k=='p':
            return est, 'pa'
        # default: return est with no hint
        return est, None
    return LOCAL_MAP.get(k, (None, None))
# Mapeo simple (expande según tu diccionario)
VARIABLE_MAP = {
    'temp_f': ('temperatura', 'f'),
    'temp': ('temperatura', 'c'),
    't': ('temperatura', 'c'),
    'temperature': ('temperatura', 'c'),
    'h': ('humedad', 'percent'),
    'hum': ('humedad', 'percent'),
    'humidity': ('humedad', 'percent'),
    'p': ('presion', 'pa'),
    'pa': ('presion', 'pa'),
    'pressure': ('presion', 'pa'),
    'radiacion': ('radiacion_solar','w_m2'),
    'rad': ('radiacion_solar','w_m2'),
    'wind_speed': ('velocidad_viento','m_s'),
    'wind': ('velocidad_viento','m_s'),
}

RANGES = {
    'temperatura': (-60, 60),    # °C
    'humedad': (0, 100),         # %
    'presion': (300, 1100),      # hPa
    'radiacion_solar': (0, 2000),
    'velocidad_viento': (0, 100)
}

def parse_timestamp(row):
    for k in ['time','timestamp','ts','datetime','date']:
        if k in row and pd.notna(row[k]):
            try:
                return pd.to_datetime(row[k], utc=True)
            except Exception:
                continue
    # fallback: now UTC
    return pd.Timestamp.now(tz=timezone.utc)

def apply_conversions(field_key, value):
    """
    Return (normalized_field_name, normalized_value)
    """
    key = str(field_key).lower()
    if key in VARIABLE_MAP:
        out, hint = VARIABLE_MAP[key]
        val = float(value)
        if out == 'temperatura':
            if hint == 'f':
                return out, round(f_to_c(val), 3)
            else:
                return out, round(val, 3)
        if out == 'presion':
            # input in Pa -> convert to hPa
            if hint == 'pa':
                return out, round(pa_to_hpa(val), 3)
            else:
                return out, round(val, 3)
        if out == 'humedad':
            # if between 0 and 1 assume fraction
            if 0 <= val <= 1:
                return out, round(decimal_to_percent(val), 3)
            return out, round(val, 3)
        # for others
        return out, round(val, 3)
    # unknown
    return None, None

def validate_values(normalized):
    flags = []
    for k,v in normalized.items():
        if k in RANGES and v is not None:
            lo, hi = RANGES[k]
            if v < lo or v > hi:
                flags.append({'tipo': 'RANGE', 'descripcion': f'{k}={v} fuera de rango [{lo},{hi}]'})
    return flags

def normalize_row(row):
    """
    Input: raw dict (row)
    Output: dict ready to insert in medicion:
      { sensor_id, timestamp (datetime tz-aware),
        temperatura, humedad, presion, radiacion_solar, velocidad_viento,
        raw: original dict,
        validation_flags: [ {tipo, descripcion}, ... ] }
    """
    ts = parse_timestamp(row)
    normalized = {
        'temperatura': None,
        'humedad': None,
        'presion': None,
        'radiacion_solar': None,
        'velocidad_viento': None
    }

    for k,v in row.items():
        if k in ['sensor_id','time','timestamp','ts','datetime','lat','lon']:
            continue
        if pd.isna(v):
            continue
        out_k, out_v = apply_conversions(k, v)
        if out_k:
            normalized[out_k] = out_v

    flags = validate_values(normalized)

    result = {
        'sensor_id': row.get('sensor_id') or row.get('sensor') or 'unknown',
        'timestamp': ts.to_pydatetime(),
        'temperatura': normalized['temperatura'],
        'humedad': normalized['humedad'],
        'presion': normalized['presion'],
        'radiacion_solar': normalized['radiacion_solar'],
        'velocidad_viento': normalized['velocidad_viento'],
        'raw': row,
        'validation_flags': flags
    }
    return result
