# commands
## list: python main.py list --bucket my-bucket
## count: python main.py count --bucket my-bucket
## read: python main.py read --bucket my-bucket --file data.csv --style fancy_grid

## put: python main.py put --bucket my-bucket --local /path/to/local/file
## python main.py -h
import argparse
import utils
from minio_operations import MinioClient
import config

def main():
    # Configuration principale du parser
    parser = argparse.ArgumentParser(description="Outil CLI pour gérer les fichiers sur MinIO")
    subparsers = parser.add_subparsers(dest="command", help="Commandes disponibles", required=True)

    # Aide pour 'list'
    list_p = subparsers.add_parser("list", help="Lister les fichiers d'un bucket")
    list_p.add_argument("--bucket", default=config.DEFAULT_BUCKET, help="Nom du bucket cible")
    
    # Aide pour 'list-all'
    list_all_p = subparsers.add_parser("list-all", help="Lister tous les fichiers d'un bucket (sans préfixe)")
    list_all_p.add_argument("--bucket", default=config.DEFAULT_BUCKET, help="Nom du bucket cible")

    # Aide pour 'count'
    count_p = subparsers.add_parser("count", help="Compter le nombre de fichiers")
    count_p.add_argument("--bucket", default=config.DEFAULT_BUCKET, help="Nom du bucket cible")

    # Aide pour 'read'
    read_p = subparsers.add_parser("read", help="Lire et afficher un CSV ou Parquet")
    read_p.add_argument("--bucket", default=config.DEFAULT_BUCKET, help="Nom du bucket")
    read_p.add_argument("--file", required=True, help="Nom de l'objet sur MinIO")
    read_p.add_argument("--style", default="grid", help="Style tabulate (grid, fancy_grid, simple)")
    
    # Aide pour 'read-all'
    read_all_p = subparsers.add_parser("read-all", help="Lire et afficher tous les CSV ou Parquet d'un bucket")
    read_all_p.add_argument("--bucket", default=config.DEFAULT_BUCKET, help="Nom du bucket")
    read_all_p.add_argument("--style", default="grid", help="Style tabulate (grid, fancy_grid, simple)")

    # Aide pour 'put'
    put_p = subparsers.add_parser("put", help="Envoyer un fichier vers MinIO")
    put_p.add_argument("--bucket", default=config.DEFAULT_BUCKET, help="Nom du bucket")
    put_p.add_argument("--local", required=True, help="Chemin du fichier sur votre PC")
    put_p.add_argument("--remote", required=True, help="Nom cible dans le bucket")

    args = parser.parse_args()
    storage = MinioClient()

    try:
        if args.command == "list":
            files = storage.list_files(args.bucket)
            print(f"\n📂 Fichiers dans '{args.bucket}':")
            for f in files: print(f"  - {f}")
            
        elif args.command == "list-all":
            files = storage.list_all_files(args.bucket)
            print(f"\n📂 Tous les fichiers dans '{args.bucket}':")
            for f in files: print(f"  - {f}")

        elif args.command == "count":
            files = storage.list_files(args.bucket)
            print(f"\n📊 Total dans '{args.bucket}': {len(files)} fichier(s).")

        elif args.command == "read":
            ext = utils.get_extension(args.file)
            print(f"⌛ Lecture de {args.file}...")
            content = storage.get_file_content(args.bucket, args.file)
            df = utils.load_dataframe(content, ext)
            # Affichage avec tabulate
            print(utils.format_table(df, tablefmt=args.style))
            print(f"\nDimensions totales : {df.shape[0]} lignes x {df.shape[1]} colonnes")

        elif args.command == "read-all":
            files = storage.list_all_files(args.bucket)
            print(f"\n📂 Lecture de tous les fichiers dans '{args.bucket}':")
            for f in files:
                if utils.is_csv_or_parquet(f):
                    ext = utils.get_extension(f)
                    content = storage.get_file_content(args.bucket, f)
                    df = utils.load_dataframe(content, ext)
                    print(f"\n📄 {f}")
                    print(utils.format_table(df, tablefmt=args.style))
                    print(f"Dimensions : {df.shape[0]} lignes x {df.shape[1]} colonnes")

        elif args.command == "put":
            storage.put_file(args.bucket, args.remote, args.local)
            print(f"✅ Succès: '{args.local}' est maintenant '{args.remote}'")

    except Exception as e:
        print(f"❌ Erreur: {e}")

if __name__ == "__main__":
    main()
