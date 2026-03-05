from minio import Minio
import config

class MinioClient:
    def __init__(self):
        self.client = Minio(
            config.MINIO_ENDPOINT,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=config.MINIO_SECURE
        )

    def list_files(self, bucket_name, prefix=""):
        """Liste les objets dans un bucket."""
        objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    
    def list_all_files(self, bucket_name):
        """Liste tous les objets dans un bucket."""
        objects = self.client.list_objects(bucket_name, recursive=True)
        return [obj.object_name for obj in objects]

    def get_file_content(self, bucket_name, object_name):
        """Récupère les octets d'un objet."""
        response = self.client.get_object(bucket_name, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()
            
    def get_all_file_contents(self, bucket_name):
        """Récupère les octets de tous les objets dans un bucket."""
        objects = self.client.list_objects(bucket_name, recursive=True)
        contents = {}
        for obj in objects:
            contents[obj.object_name] = self.get_file_content(bucket_name, obj.object_name)
        return contents

    def put_file(self, bucket_name, object_name, file_path):
        """Envoie un fichier local vers MinIO."""
        return self.client.fput_object(bucket_name, object_name, file_path)
