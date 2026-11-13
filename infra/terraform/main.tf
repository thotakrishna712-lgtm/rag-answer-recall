# Landing zone for policy exports and Debezium snapshots consumed by Auto Loader.
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "environment" {
  type = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be dev, test or prod"
  }
}

variable "databricks_role_arn" {
  type        = string
  description = "IAM role assumed by the Unity Catalog storage credential"
}

resource "aws_kms_key" "landing" {
  description         = "ragrecall landing zone (${var.environment})"
  enable_key_rotation = true
}

resource "aws_s3_bucket" "landing" {
  bucket = "ragrecall-landing-${var.environment}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.landing.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "databricks_read" {
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.landing.arn, "${aws_s3_bucket.landing.arn}/*"]
  }
  statement {
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.landing.arn]
  }
}

resource "aws_iam_policy" "databricks_read" {
  name   = "ragrecall-landing-read-${var.environment}"
  policy = data.aws_iam_policy_document.databricks_read.json
}

resource "aws_iam_role_policy_attachment" "databricks_read" {
  role       = element(split("/", var.databricks_role_arn), length(split("/", var.databricks_role_arn)) - 1)
  policy_arn = aws_iam_policy.databricks_read.arn
}

output "landing_bucket" {
  value = aws_s3_bucket.landing.bucket
}
