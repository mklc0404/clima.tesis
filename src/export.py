# src/export.py
import xarray as xr
import pandas as pd

def export_mediciones_to_netcdf(df, out_path):
    """
    df: pandas DataFrame con columnas: timestamp, sensor_id, var_name, value
    Para nuestro caso primero pivotear por variable.
    """
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_convert('UTC')
    ds_vars = {}
    for var in ['temperatura','humedad','presion','radiacion_solar','velocidad_viento']:
        sub = df[df['variable']==var]
        if sub.empty:
            continue
        pivot = sub.pivot_table(index='timestamp', columns='sensor_id', values='value')
        ds_vars[var] = xr.DataArray(pivot, dims=('time','sensor'), coords={'time':pivot.index, 'sensor':pivot.columns})
    ds = xr.Dataset(ds_vars)
    ds.attrs['Conventions'] = 'CF-1.8'
    ds.to_netcdf(out_path)
