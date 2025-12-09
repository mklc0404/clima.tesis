# tests/test_normalize.py
from src.normalize import normalize_row

def test_temp_f_to_c():
    row = {'sensor_id':'s1','time':'2025-01-01T00:00:00Z','temp_f':77}
    out = normalize_row(row)
    assert len(out) == 1
    obs = out[0]
    # 77°F ≈ 25°C
    assert abs(obs['value'] - 25.0) < 0.5
