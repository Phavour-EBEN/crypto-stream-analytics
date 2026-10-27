terraform {
  required_version = ">= 1.6"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

# Logs in as the crypto-pipeline service principal (client secret).
# All values come from variables.tf, so no IDs are hardcoded here.
provider "azurerm" {
  features {}

  # The service principal only has rights on the resource group, not the whole
  # subscription, so it can't register providers. The ones this project needs
  # (Storage, Compute, Network, Synapse) were registered once with `az provider register`.
  resource_provider_registrations = "none"

  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
  client_id       = var.client_id
  client_secret   = var.client_secret
}

# The resource group already exists (created in the portal), so it's read, not created.
# Everything below is created inside it, but in var.location: a resource group is only
# a label, so its resources can live in a different region.
data "azurerm_resource_group" "rg" {
  name = var.resource_group_name
}

locals {
  rg_name  = data.azurerm_resource_group.rg.name
  location = var.location
}

# =============================================================================
# Resources are created ONE STEP AT A TIME.
# To create the next step: delete its /* and */ lines (and the matching ones in
# outputs.tf, if any), then run `terraform plan` and `terraform apply`.
# =============================================================================

# ---------- Data lake ----------

# STEP 1 - Storage account (Data Lake Gen2)
resource "azurerm_storage_account" "lake" {
  name                     = var.storage_account_name
  resource_group_name      = local.rg_name
  location                 = local.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # hierarchical namespace = Data Lake Gen2 (real folders)
}

# STEP 2 - "lake" container: Spark writes its Parquet files here

resource "azurerm_storage_data_lake_gen2_filesystem" "lake" {
  name               = "lake"
  storage_account_id = azurerm_storage_account.lake.id
}


# STEP 3 - "synapse" container: Synapse needs a default filesystem of its own

resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  name               = "synapse"
  storage_account_id = azurerm_storage_account.lake.id
}


# ---------- Data warehouse ----------

# STEP 4 - Synapse workspace (needs step 3). Takes ~5-10 minutes.

resource "azurerm_synapse_workspace" "ws" {
  name                                 = var.synapse_workspace_name
  resource_group_name                  = local.rg_name
  location                             = local.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse.id
  sql_administrator_login              = var.sql_admin_user
  sql_administrator_login_password     = var.sql_admin_password

  identity {
    type = "SystemAssigned" # Synapse's own identity; serverless SQL uses it to read the lake
  }
}

# No SQL pool resource: the workspace comes with a built-in serverless SQL pool
# (<workspace>-ondemand.sql.azuresynapse.net). It queries the Parquet files in the lake
# directly and bills only per TB scanned, so nothing costs money while idle.
# Its database (cryptodw) is created later with SQL, not Terraform.

# STEP 5 - Synapse firewall rules (need step 4)

# Lets the Power BI service and other Azure services connect
resource "azurerm_synapse_firewall_rule" "azure_services" {
  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.ws.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}

# Lets your machine connect (sqlcmd, dbt, Power BI Desktop)
resource "azurerm_synapse_firewall_rule" "client" {
  name                 = "AllowClientIp"
  synapse_workspace_id = azurerm_synapse_workspace.ws.id
  start_ip_address     = var.client_ip
  end_ip_address       = var.client_ip
}


# Role assignments are NOT created here: crypto-pipeline is a Contributor, which can't
# grant roles. They're created once with `az role assignment create`, run as you.

# ---------- VM that runs producer + Kafka + Spark ----------

# STEP 6 - Virtual network

resource "azurerm_virtual_network" "vnet" {
  name                = "crypto-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = local.location
  resource_group_name = local.rg_name
}


# STEP 7 - Subnet (needs step 6)

resource "azurerm_subnet" "subnet" {
  name                 = "default"
  resource_group_name  = local.rg_name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}


# STEP 8 - Public IP for the VM

resource "azurerm_public_ip" "vm" {
  name                = "crypto-vm-ip"
  location            = local.location
  resource_group_name = local.rg_name
  allocation_method   = "Static"
  sku                 = "Standard"
}


# STEP 9 - Firewall (NSG): only your IP may SSH in. Outbound traffic is allowed by default.

resource "azurerm_network_security_group" "vm" {
  name                = "crypto-vm-nsg"
  location            = local.location
  resource_group_name = local.rg_name

  security_rule {
    name                       = "SSH"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = var.client_ip
    destination_address_prefix = "*"
  }
}


# STEP 10 - Network card (needs steps 7 and 8)

resource "azurerm_network_interface" "vm" {
  name                = "crypto-vm-nic"
  location            = local.location
  resource_group_name = local.rg_name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.vm.id
  }
}


# STEP 11 - Attach the firewall to the network card (needs steps 9 and 10)

resource "azurerm_network_interface_security_group_association" "vm" {
  network_interface_id      = azurerm_network_interface.vm.id
  network_security_group_id = azurerm_network_security_group.vm.id
}


# STEP 12 - The VM (needs step 10 and an SSH key at ~/.ssh/id_rsa.pub). Bills while running.

resource "azurerm_linux_virtual_machine" "stream_vm" {
  name                  = "crypto-stream-vm"
  resource_group_name   = local.rg_name
  location              = local.location
  size                  = var.vm_size
  admin_username        = var.vm_admin_username
  network_interface_ids = [azurerm_network_interface.vm.id]

  admin_ssh_key {
    username   = var.vm_admin_username
    public_key = file(pathexpand(var.ssh_public_key_path))
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
    disk_size_gb         = 30
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }
}

