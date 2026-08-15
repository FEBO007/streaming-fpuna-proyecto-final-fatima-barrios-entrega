$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$group = "velocity-alert-materializer-$timestamp"

& .\.venv\Scripts\python.exe -m src.materializer.alert_consumer `
  --bootstrap-servers localhost:9092 `
  --topic risk.alerts.velocity `
  --consumer-group $group `
  --from-beginning `
  --max-messages 3 `
  --timeout-seconds 10 `
  --state-db .\data\materialized-alerts.sqlite3

exit $LASTEXITCODE
