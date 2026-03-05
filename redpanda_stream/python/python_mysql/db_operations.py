# db_operations.py
# pip install --no-cache-dir mysql-connector-python

import mysql.connector

def get_connection(config):
    return mysql.connector.connect(**config)

def test_connection(cursor):
    """Vérifie la connexion en récupérant la version du serveur MySQL."""
    cursor.execute("SELECT VERSION()")
    version = cursor.fetchone()
    return version[0] if version else None

def add_client(cursor, code, name, actif=True):
    query = "INSERT INTO client (code, name, actif) VALUES (%s, %s, %s)"
    cursor.execute(query, (code, name, actif))
    return cursor.lastrowid

def add_product(cursor, code, name, pu, actif=True):
    query = "INSERT INTO product (code, name, actif, pu) VALUES (%s, %s, %s, %s)"
    cursor.execute(query, (code, name, actif, pu))
    return cursor.lastrowid

def add_sale(cursor, id_client, id_product, qte, total):
    query = "INSERT INTO sales (id_client, id_product, qte, total) VALUES (%s, %s, %s, %s)"
    cursor.execute(query, (id_client, id_product, qte, total))
    return cursor.lastrowid

def fetch_all(cursor, table_name):
    """Récupère toutes les lignes d'une table donnée."""
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    return cursor.fetchall()
