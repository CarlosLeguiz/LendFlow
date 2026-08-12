output "data_bucket_name" {
  description = "Nombre del bucket S3 para data"
  value       = aws_s3_bucket.data.bucket
}

output "athena_results_bucket_name" {
  description = "Nombre del bucket S3 para resultados de Athena"
  value       = aws_s3_bucket.athena_results.bucket
}

output "glue_database_name" {
  description = "Nombre de la base de datos en Glue Catalog"
  value       = aws_glue_catalog_database.lendflow.name
}