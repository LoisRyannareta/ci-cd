param(
    [string]$ResourceGroup = "CI-CD",
    [string]$StorageAccount = "cloudnim06",
    [string]$UserId = "cbaab661-a7d6-4939-84a5-c837bde0ced1"
)

$ErrorActionPreference = "Stop"

$scope = az storage account show --name $StorageAccount --resource-group $ResourceGroup --query id -o tsv
if (-not $scope) {
    throw "Storage account $StorageAccount tidak ditemukan di resource group $ResourceGroup."
}

Write-Host "Memberi Storage Blob Data Contributor ke $UserId ..."
az role assignment create `
    --assignee $UserId `
    --role "Storage Blob Data Contributor" `
    --scope $scope `
    --only-show-errors | Out-Null

Write-Host "Role saat ini:"
az role assignment list --assignee $UserId --scope $scope `
    --query "[].{role:roleDefinitionName,scope:scope}" -o table
