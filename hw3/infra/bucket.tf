resource "yandex_storage_bucket" "data_bucket" {
  bucket        = "${var.yc_bucket_name}"
  access_key    = yandex_iam_service_account_static_access_key.service_account_static_key.access_key
  secret_key    = yandex_iam_service_account_static_access_key.service_account_static_key.secret_key
  force_destroy = true
}