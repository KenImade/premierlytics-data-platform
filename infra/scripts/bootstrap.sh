#!/bin/bash
set -euo pipefail

exec > /var/log/pipeline.log 2>&1

echo "=== Pipeline starting at $$(date -u) ==="

# Install Docker
yum update -y
yum install -y docker aws-cli
systemctl start docker

# Login to ECR
aws ecr get-login-password --region ${aws_region} | \
    docker login --username AWS --password-stdin ${ecr_registry}

# Create working directory
mkdir -p /data/duckdb

# Download persistent DuckDB database from S3
echo "Downloading DuckDB data from S3..."
aws s3 sync s3://${s3_bucket}/duckdb/ /data/duckdb/ || true

# Pull pipeline image
docker pull ${ecr_repo}:latest

# Run pipeline
# --network host allows the container to reach the EC2 instance metadata service
# so boto3 can pick up IAM role credentials automatically
docker run --rm \
    --network host \
    -v /data/duckdb:/data/duckdb \
    -e S3_BUCKET_NAME=${s3_bucket} \
    -e AWS_REGION=${aws_region} \
    -e DUCKDB_PATH=/data/duckdb/premierlytics.duckdb \
    -e SUPABASE_CONNECTION_STRING="${supabase_connection_string}" \
    -e DAGSTER_ENVIRONMENT=prod \
    ${ecr_repo}:latest \
    python -m premierlytics_dagster.run_pipeline

echo "Pipeline completed at $$(date -u)"

# Upload DuckDB database back to S3
echo "Uploading DuckDB data to S3..."
aws s3 sync /data/duckdb/ s3://${s3_bucket}/duckdb/

# Upload logs
aws s3 cp /var/log/pipeline.log s3://${s3_bucket}/logs/pipeline-$$(date +%%Y%%m%%d-%%H%%M%%S).log

echo "=== Shutting down ==="

# Self-terminate (IMDSv2)
TOKEN=$$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$$(curl -s -H "X-aws-ec2-metadata-token: $$TOKEN" \
    http://169.254.169.254/latest/meta-data/instance-id)
aws ec2 terminate-instances --instance-ids $$INSTANCE_ID --region ${aws_region}
