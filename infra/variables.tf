variable "aws_region" {
  description = "AWS region"
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Project name used for resource naming"
  default     = "premierlytics"
}

variable "instance_type" {
  description = "EC2 instance type"
  default     = "t3.small"
}

variable "supabase_connection_string" {
  description = "Supabase PostgreSQL connection string"
  sensitive   = true
}
