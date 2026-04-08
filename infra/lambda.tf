data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/scripts/lambda_function.py"
  output_path = "${path.module}/scripts/lambda_function.zip"
}

resource "aws_lambda_function" "start_pipeline" {
  function_name    = "${var.project_name}-start-pipeline"
  role             = aws_iam_role.lambda_starter.arn
  handler          = "lambda_function.handler"
  runtime          = "python3.13"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      LAUNCH_TEMPLATE_NAME = aws_launch_template.pipeline.name
      DEPLOY_REGION        = var.aws_region
    }
  }
}
