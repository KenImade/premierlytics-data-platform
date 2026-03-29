import pytest
from testcontainers.minio import MinioContainer

from premierlytics_dagster.defs.resources.minio import MinioResource


@pytest.fixture(scope="session")
def minio_container():
    with MinioContainer() as container:
        yield container


@pytest.fixture
def minio_resource(minio_container) -> MinioResource:
    client = minio_container.get_client()
    bucket = "test-bucket"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    host = minio_container.get_container_host_ip()
    port = minio_container.get_exposed_port(9000)

    return MinioResource(
        endpoint=f"{host}:{port}",
        access_key=minio_container.access_key,
        secret_key=minio_container.secret_key,
        secure=False,
        bucket=bucket,
    )


def test_put_and_get_csv(minio_resource: MinioResource):
    csv_text = "id,name\n1,alice\n2,bob\n"
    minio_resource.put_object(key="test/data.csv", data=csv_text)
    result = minio_resource.get_object(key="test/data.csv")
    assert result == csv_text


def test_put_and_get_bytes(minio_resource: MinioResource):
    data = b"\x00\x01\x02\x03"
    minio_resource.put_object(
        key="test/data.parquet", data=data, content_type="application/octet-stream"
    )
    result = minio_resource.get_bytes(key="test/data.parquet")
    assert result == data


def test_list_objects(minio_resource: MinioResource):
    minio_resource.put_object(key="prefix/a.csv", data="a")
    minio_resource.put_object(key="prefix/b.csv", data="b")
    minio_resource.put_object(key="other/c.csv", data="c")

    keys = minio_resource.list_objects(prefix="prefix/")
    assert sorted(keys) == ["prefix/a.csv", "prefix/b.csv"]
