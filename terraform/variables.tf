# Login details for the crypto-pipeline service principal.
# Values come from TF_VAR_* environment variables (loaded from .env), never from this file.

variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "tenant_id" {
  description = "Directory (tenant) ID of the service principal"
  type        = string
}

variable "client_id" {
  description = "Application (client) ID of the crypto-pipeline service principal"
  type        = string
}

variable "client_secret" {
  description = "Client secret of the crypto-pipeline service principal"
  type        = string
  sensitive   = true
}

variable "resource_group_name" {
  description = "Existing resource group that holds the project"
  type        = string
  default     = "rg-crypto-stream"
}

variable "location" {
  description = "Region for all resources (non-US so Binance works; West Europe rejects new subscriptions)"
  type        = string
  default     = "southafricanorth"
}

# ---------- Data lake + warehouse ----------

variable "storage_account_name" {
  description = "Data lake storage account; globally unique, 3-24 lowercase letters/numbers"
  type        = string
}

variable "synapse_workspace_name" {
  description = "Synapse workspace; globally unique, lowercase letters/numbers/hyphens"
  type        = string
}

variable "sql_admin_user" {
  description = "Synapse SQL admin login (dbt and Power BI use it)"
  type        = string
  default     = "sqladminuser"
}

variable "sql_admin_password" {
  description = "Synapse SQL admin password (8+ chars with upper, lower, number, symbol)"
  type        = string
  sensitive   = true
}

variable "client_ip" {
  description = "Your current public IP; allowed to SSH to the VM and connect to Synapse"
  type        = string
}

# ---------- VM ----------

variable "vm_size" {
  description = "2 vCPU / 8 GB: fits the 2 free vCPUs in the region. Use Standard_D4s_v5 (16 GB) if quota allows"
  type        = string
  default     = "Standard_D2s_v5"
}

variable "vm_admin_username" {
  description = "Linux user you SSH in as"
  type        = string
  default     = "azureuser"
}

variable "ssh_public_key_path" {
  description = "Public key used to log in to the VM"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}
