"""Prefect flow for moving data from Bronze to Silver layer."""

from prefect import flow


@flow(name="bronze-to-silver")
def bronze_to_silver() -> None:
    pass


if __name__ == "__main__":
    bronze_to_silver()
