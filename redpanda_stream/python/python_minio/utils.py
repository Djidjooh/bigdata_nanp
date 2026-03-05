import pandas as pd
import io
import os
from tabulate import tabulate

def load_dataframe(raw_data, extension):
    """Charge un DF selon l'extension."""
    if extension == '.csv':
        return pd.read_csv(io.BytesIO(raw_data))
    elif extension == '.parquet':
        return pd.read_parquet(io.BytesIO(raw_data))
    raise ValueError(f"Format {extension} non supporté.")

def format_table(df, tablefmt="grid"):
    # tablefmt options: "plain", "simple", "github", "grid", "fancy_grid", "pipe", "orgtbl", "jira", "presto", "psql", "rst", "mediawiki", "html", "latex", "latex_raw", "latex_booktabs"
    """Formate le DataFrame pour un affichage console propre."""
    # On affiche les 10 premières lignes par défaut
    return tabulate(df.head(10), headers='keys', tablefmt=tablefmt, showindex=False)

def get_extension(filename):
    return os.path.splitext(filename)[1].lower()

def is_csv_or_parquet(filename):
    ext = get_extension(filename)
    return ext in ['.csv', '.parquet']  
