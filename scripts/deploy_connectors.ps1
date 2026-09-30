# PowerShell Runner for Connector Deployment
Write-Host "Deploying connectors with environment variable interpolation..." -ForegroundColor Cyan
python "$PSScriptRoot/deploy_connectors.py"
