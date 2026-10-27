output "resource_group_name" {
  value = data.azurerm_resource_group.rg.name
}

output "resource_group_location" {
  value = data.azurerm_resource_group.rg.location
}

output "resource_group_id" {
  value = data.azurerm_resource_group.rg.id
}

# STEP 1 - Storage account
output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "storage_account_id" {
  description = "Scope for the lake role assignments"
  value       = azurerm_storage_account.lake.id
}

# STEP 4 - Synapse workspace (uncomment together with step 4 in main.tf)

output "synapse_workspace_name" {
  value = azurerm_synapse_workspace.ws.name
}

output "synapse_sql_endpoint" {
  description = "Serverless SQL endpoint (dbt, sqlcmd and Power BI connect here)"
  value       = "${azurerm_synapse_workspace.ws.name}-ondemand.sql.azuresynapse.net"
}

output "synapse_principal_id" {
  description = "Synapse's managed identity; needs Storage Blob Data Contributor on the lake"
  value       = azurerm_synapse_workspace.ws.identity[0].principal_id
}


# STEP 8 - Public IP (uncomment together with step 8 in main.tf)

output "vm_public_ip" {
  value = azurerm_public_ip.vm.ip_address
}

