# Configuracion del provider de AWS.
# Le dice a Terraform "voy a crear recursos en AWS, region Ohio".
terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --------------------------------------------------------------------------
# S3: Bucket principal para data (bronze / silver / gold)
# --------------------------------------------------------------------------
resource "aws_s3_bucket" "data" {
  bucket = "${var.project_name}-data-${var.aws_account_id}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Bloquear acceso publico al bucket de data (seguridad basica)
resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --------------------------------------------------------------------------
# S3: Bucket para resultados de Athena
# Athena necesita un bucket donde escribir los resultados de las queries.
# --------------------------------------------------------------------------
resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-athena-results-${var.aws_account_id}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --------------------------------------------------------------------------
# Glue: Base de datos del catalogo
# Es donde Athena y Spark van a buscar las definiciones de tablas.
# --------------------------------------------------------------------------
resource "aws_glue_catalog_database" "lendflow" {
  name = "${var.project_name}_${var.environment}"

  description = "Catalogo de tablas para el proyecto LendFlow (${var.environment})"
}