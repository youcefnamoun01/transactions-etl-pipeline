import pandas as pd

def read_file(path: str) -> pd.DataFrame:
    if path.endswith(".xlsx"):
        return pd.read_excel(path)
    elif path.endswith(".csv"):
        return pd.read_csv(path)
    else:
        raise ValueError("Format non supporté (seulement .csv et .xlsx)")

def write_file(df: pd.DataFrame, path: str) -> None:
    if path.endswith(".xlsx"):
        df.to_excel(path, index=False)
    elif path.endswith(".csv"):
        df.to_csv(path, index=False)
    else:
        raise ValueError("Format non supporté (seulement .csv et .xlsx)")
