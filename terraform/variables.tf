variable "aws_region" {
  description = "Region de AWS donde se crean los recursos"
  type        = string
  default     = "us-east-2"
}

variable "aws_account_id" {
  description = "ID de la cuenta AWS (para nombres unicos de buckets)"
  type        = string
  default     = "851563823943"
}

variable "project_name" {
  description = "Nombre del proyecto, se usa como prefijo en todos los recursos"
  type        = string
  default     = "lendflow"
}

variable "environment" {
  description = "Ambiente: dev, staging, prod"
  type        = string
  default     = "dev"
}