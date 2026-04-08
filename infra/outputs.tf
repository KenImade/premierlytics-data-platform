output "s3_bucket" {
  value = aws_s3_bucket.pipeline_data.id
}

output "ecr_repo_url" {
  value = aws_ecr_repository.dagster.repository_url
}

output "lambda_function" {
  value = aws_lambda_function.start_pipeline.function_name
}
