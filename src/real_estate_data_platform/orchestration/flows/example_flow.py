from prefect import flow, get_run_logger, task

from real_estate_data_platform.config.settings import settings


@task
def process_data(minio_endpoint: str):
    logger = get_run_logger()
    logger.info(f"Conectando a {minio_endpoint}")
    # ... lógica ...


@flow
def ingest_listings_flow(city: str = "toronto"):
    logger = get_run_logger()

    minio_endpoint = settings.minio.endpoint
    minio_secret_key = settings.minio.secret_key.get_secret_value()
    db_port = settings.postgres.port
    db_dsn = settings.postgres.dsn

    logger.info(f"MinIO endpoint: {minio_endpoint}")
    logger.info(f"MinIO secret key: {minio_secret_key}")
    logger.info(f"PostgreSQL port: {db_port}")
    logger.info(f"PostgreSQL DSN: {db_dsn}")
    logger.info(f"Ciudad: {city}")

    # 3. Pasar settings y credenciales a las tareas
    process_data(minio_endpoint)


if __name__ == "__main__":
    ingest_listings_flow()
