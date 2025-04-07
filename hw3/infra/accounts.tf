resource "yandex_iam_service_account" "service_account_hw3" {
  name        = var.yc_service_account_name
  folder_id   = var.folder_id
  description = "service account to manage Dataproc Cluster"
}

resource "yandex_resourcemanager_folder_iam_member" "service_account_hw3_roles" {
  for_each = toset([
    "storage.admin",
    "dataproc.editor",
    "compute.admin",
    "dataproc.agent",
    "mdb.dataproc.agent",
    "vpc.user",
    "iam.serviceAccounts.user",
    "storage.uploader",
    "storage.viewer",
    "storage.editor"
  ])

  folder_id = var.folder_id
  role      = each.key
  member    = "serviceAccount:${yandex_iam_service_account.service_account_hw3.id}"
}

resource "yandex_iam_service_account_static_access_key" "service_account_static_key" {
  service_account_id = yandex_iam_service_account.service_account_hw3.id
  description        = "Static access key for object storage"
}

output "s3_access_key" {
  value = yandex_iam_service_account_static_access_key.service_account_static_key.access_key
  sensitive = true
}

output "s3_secret_key" {
  value = yandex_iam_service_account_static_access_key.service_account_static_key.secret_key
  sensitive = true
}