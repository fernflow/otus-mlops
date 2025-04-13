variable "cloud_id" {
  description = "Yandex Cloud ID"
  type        = string
  sensitive   = true
}

variable "folder_id" {
  description = "Yandex Cloud Folder ID"
  type        = string
  sensitive   = true
}

variable "yc_service_account_name" {
  description = "Yandex Cloud Service Account Name"
  type        = string
}

variable "yc_network_name" {
  description = "Yandex Cloud Network Name"
  type        = string
}

variable "yc_subnet_name" {
  description = "Yandex Cloud Subnet Name"
  type        = string
}

variable "yc_zone" {
  description = "Yandex Cloud Zone"
  type        = string
}

variable "yc_subnet_range" {
  description = "Yandex Cloud Subnet Range"
  type        = string
}

variable "yc_nat_gateway_name" {
  description = "Yandex Cloud Nat Gateway Name"
  type        = string
}

variable "yc_route_table_name" {
  description = "Yandex Cloud Route Table Name"
  type        = string
}

variable "yc_security_group_name" {
  description = "Yandex Cloud Security Group Name"
  type        = string
}

variable "yc_bucket_name" {
  description = "Yandex Cloud Bucket Name"
  type        = string
}

variable "yc_dataproc_cluster_name" {
  description = "Yandex Cloud Dataproc Cluster Name"
  type        = string
}

variable "public_key_path" {
  description = "Public Key Path SSH"
  type        = string
}

variable "dataproc_master_resources" {
  type = object({
    resource_preset_id = string
    disk_type_id       = string
    disk_size          = number
  })

  default = {
    resource_preset_id = "s3-c2-m8"
    disk_type_id       = "network-ssd"
    disk_size          = 40
  }
}

variable "dataproc_compute_resources" {
  type = object({
    resource_preset_id = string
    disk_type_id       = string
    disk_size          = number
  })

  default = {
    resource_preset_id = "s3-c4-m16"
    disk_type_id       = "network-ssd"
    disk_size          = 128
  }
}

  variable "dataproc_data_resources" {
    type = object({
      resource_preset_id = string
      disk_type_id       = string
      disk_size          = number
    })

    default = {
      resource_preset_id = "s3-c4-m16"
      disk_type_id       = "network-ssd"
      disk_size          = 128
    }
}
