# utils.py
# pip install --no-cache-dir mysql-connector-python tabulate

from tabulate import tabulate

def print_table(rows, headers, table_name):
    """Affiche les données sous forme de tableau stylisé."""
    print(f"\n=== TABLE: {table_name.upper()} ===")
    if not rows:
        print("Aucune donnée disponible.")
    else:
        # On utilise le format 'grid' pour un rendu type base de données
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    print("\n")
