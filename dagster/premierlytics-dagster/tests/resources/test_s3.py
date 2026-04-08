import pytest
import boto3
from moto import mock_aws

from premierlytics_dagster.defs.resources.s3 import S3Resource

BUCKET = "test-bucket"
REGION = "eu-west-2"


@pytest.fixture
def s3_resource():
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
        yield S3Resource(bucket=BUCKET, region=REGION)


def test_put_and_get_csv(s3_resource: S3Resource):
    csv_text = "id,name\n1,alice\n2,bob\n"
    s3_resource.put_object(key="test/data.csv", data=csv_text)
    result = s3_resource.get_object(key="test/data.csv")
    assert result == csv_text


def test_put_and_get_bytes(s3_resource: S3Resource):
    data = b"\x00\x01\x02\x03"
    s3_resource.put_object(
        key="test/data.parquet", data=data, content_type="application/octet-stream"
    )
    result = s3_resource.get_bytes(key="test/data.parquet")
    assert result == data


def test_list_objects(s3_resource: S3Resource):
    s3_resource.put_object(key="prefix/a.csv", data="a")
    s3_resource.put_object(key="prefix/b.csv", data="b")
    s3_resource.put_object(key="other/c.csv", data="c")

    keys = s3_resource.list_objects(prefix="prefix/")
    assert sorted(keys) == ["prefix/a.csv", "prefix/b.csv"]
