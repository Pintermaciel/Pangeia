import boto3


class S3Manager:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')

    def upload_file(self, file_path, s3_key):
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            print(f"Arquivo {file_path} enviado para o S3 com a chave {s3_key}")
        except Exception as e:
            print(f"Erro ao enviar arquivo para o S3: {e}")
