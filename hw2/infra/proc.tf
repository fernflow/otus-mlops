resource "yandex_dataproc_cluster" "dataproc_cluster" {
  name        = var.yc_dataproc_cluster_name
  depends_on  = [ yandex_resourcemanager_folder_iam_member.service_account_hw2_roles ]
  folder_id   = var.folder_id
  bucket      = yandex_storage_bucket.data_bucket.bucket
  description = "Dataproc Cluster created by Terraform"
  labels = {
    created_by = "terraform"
  }
  service_account_id = yandex_iam_service_account.service_account_hw2.id
  zone_id            = var.yc_zone
  security_group_ids = [ yandex_vpc_security_group.security_group.id ]

  cluster_config {

    hadoop {
      services = ["HDFS", "YARN", "SPARK", "TEZ", "HIVE"]
      properties = {
        "yarn:yarn.resourcemanager.am.max-attempts" = 5
      }
      
      ssh_public_keys = [
      file(var.public_key_path)]
    }

    subcluster_spec {
      name = "master"
      role = "MASTERNODE"
      resources {
        resource_preset_id = var.dataproc_master_resources.resource_preset_id
        disk_type_id       = "network-hdd"
        disk_size          = var.dataproc_master_resources.disk_size
      }
      subnet_id        = yandex_vpc_subnet.subnet.id
      hosts_count      = 1
      assign_public_ip = true
    }

    subcluster_spec {
      name = "data"
      role = "DATANODE"
      resources {
        resource_preset_id = var.dataproc_data_resources.resource_preset_id
        disk_type_id       = "network-hdd"
        disk_size          = var.dataproc_data_resources.disk_size
      }
      subnet_id   = yandex_vpc_subnet.subnet.id
      hosts_count = 3
    }
  }
}