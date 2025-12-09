# src/ingest.py
import pandas as pd

def read_csv(path):
    try:
        return pd.read_csv(path, encoding='utf-8')
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding='latin-1')

def rows_from_df(df):
    for _, r in df.iterrows():
        yield r.to_dict()
