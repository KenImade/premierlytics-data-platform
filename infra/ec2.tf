data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}


resource "aws_security_group" "pipeline" {
  name        = "${var.project_name}-pipeline"
  description = "Allow outbound traffic for pipeline"

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project = var.project_name
  }
}

resource "aws_launch_template" "pipeline" {
  name          = "${var.project_name}-pipeline"
  image_id      = data.aws_ami.amazon_linux.id
  instance_type = var.instance_type

  iam_instance_profile {
    name = aws_iam_instance_profile.ec2_pipeline.name
  }

  vpc_security_group_ids = [aws_security_group.pipeline.id]

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  instance_market_options {
    market_type = "spot"
    spot_options {
      max_price                      = "0.02"
      instance_interruption_behavior = "terminate"
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name    = "${var.project_name}-pipeline-runner"
      Project = var.project_name
    }
  }

  user_data = base64encode(templatefile("${path.module}/scripts/bootstrap.sh", {
    aws_region                 = var.aws_region
    s3_bucket                  = aws_s3_bucket.pipeline_data.id
    ecr_repo                   = aws_ecr_repository.dagster.repository_url
    ecr_registry               = "${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
    project_name               = var.project_name
    supabase_connection_string = var.supabase_connection_string
  }))
}
