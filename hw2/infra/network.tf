resource "yandex_vpc_network" "network" {
  name        = var.yc_network_name
  folder_id   = var.folder_id
  description = "Dataproc network"
}

resource "yandex_vpc_subnet" "subnet" {
  name           = var.yc_subnet_name
  zone           = var.yc_zone
  network_id     = yandex_vpc_network.network.id
  v4_cidr_blocks = [var.yc_subnet_range]
  folder_id      = var.folder_id
  route_table_id = yandex_vpc_route_table.route_table.id
  description    = "Dataproc subnet"
}

resource "yandex_vpc_gateway" "nat_gateway" {
  name       = var.yc_nat_gateway_name
  folder_id  = var.folder_id
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "route_table" {
  name       = var.yc_route_table_name
  folder_id  = var.folder_id
  network_id = yandex_vpc_network.network.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.nat_gateway.id
  }
}

resource "yandex_vpc_security_group" "security_group" {
  name        = var.yc_security_group_name
  network_id  = yandex_vpc_network.network.id
  folder_id   = var.folder_id
  description = "Security group for the Yandex Data Processing cluster"

  ingress {
    protocol       = "ANY"
    description    = "Allow all incoming traffic"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    protocol       = "TCP"
    description    = "UI"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 443
  }

  ingress {
    protocol       = "TCP"
    description    = "SSH"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 22
  }

  ingress {
    protocol       = "TCP"
    description    = "Jupyter"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 8888
  }

  egress {
    protocol       = "ANY"
    description    = "Allow all outgoing traffic"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol       = "ANY"
    description    = "Allow all outgoing traffic"
    from_port      = 0
    to_port        = 65535
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol          = "ANY"
    description       = "Internal"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }
}
