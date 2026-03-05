# main.py
# run
## python main.py --help

## python main.py client --code "C003" --name "Entreprise XYZ"
## python main.py product --code "P-TAB" --name "Tablette" --pu 299.99
## python main.py sale --client_id 1 --product_id 2 --qte 3 --total 899.97

## python main.py list client
## python main.py list product
## python main.py list sales

## python main.py test

import argparse
import db_operations as db
from config import DB_CONFIG
import utils 

def main():
    parser = argparse.ArgumentParser(description="CLI TYROK - Gestion de la base de données")
    subparsers = parser.add_subparsers(dest="command", help="Commandes disponibles")
    
    # Commande pour ajouter un client
    parser_client = subparsers.add_parser('client', help="Ajouter un client")
    parser_client.add_argument('--code', required=True, help="Code du client")
    parser_client.add_argument('--name', required=True, help="Nom du client")
    parser_client.add_argument('--actif', type=int, default=1, help="1 pour actif, 0 sinon")

    # Commande pour ajouter un produit
    parser_prod = subparsers.add_parser('product', help="Ajouter un produit")
    parser_prod.add_argument('--code', required=True)
    parser_prod.add_argument('--name', required=True)
    parser_prod.add_argument('--pu', type=float, required=True, help="Prix unitaire")

    # Commande pour ajouter une vente
    parser_sale = subparsers.add_parser('sale', help="Enregistrer une vente")
    parser_sale.add_argument('--client_id', type=int, required=True)
    parser_sale.add_argument('--product_id', type=int, required=True)
    parser_sale.add_argument('--qte', type=int, required=True)
    parser_sale.add_argument('--total', type=float, required=True)
    
    # Commande pour lister les données
    parser_list = subparsers.add_parser('list', help="Afficher le contenu d'une table")
    parser_list.add_argument('table', choices=['client', 'product', 'sales'], help="Nom de la table")

    # commande pour tester la connexion
    subparsers.add_parser('test', help="Tester la connexion à la base de données")                                    

    args = parser.parse_args()
    conn = None

    try:
        conn = db.get_connection(DB_CONFIG)
        cursor = conn.cursor()
        
        if args.command == 'test':
            version = db.test_connection(cursor)
            if version:
                print(f"✅ Connexion réussie ! Version MySQL : {version}")
            else:
                print("❌ Échec de la récupération de la version.")
        
        if args.command == 'list':
            rows = db.fetch_all(cursor, args.table)
            if not rows:
                print(f"La table '{args.table}' est vide.")
            else:
                # cursor.description contient les noms des colonnes
                headers = [i[0] for i in cursor.description] 
                utils.print_table(rows, headers, args.table)
                

        if args.command == 'client':
            new_id = db.add_client(cursor, args.code, args.name, bool(args.actif))
            print(f"Client ajouté ! ID: {new_id}")
        
        elif args.command == 'product':
            new_id = db.add_product(cursor, args.code, args.name, args.pu)
            print(f"Produit ajouté ! ID: {new_id}")
            
        elif args.command == 'sale':
            new_id = db.add_sale(cursor, args.client_id, args.product_id, args.qte, args.total)
            print(f"Vente enregistrée ! ID: {new_id}")

        conn.commit()
    except Exception as e:
        print(f"Erreur : {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()
