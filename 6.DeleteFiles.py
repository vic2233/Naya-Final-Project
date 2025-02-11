from minio import Minio

# MinIO Configuration
minio_client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "finalproject"

if minio_client.bucket_exists(bucket_name):
    objects = minio_client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        minio_client.remove_object(bucket_name, obj.object_name)

#    minio_client.remove_bucket(bucket_name)
