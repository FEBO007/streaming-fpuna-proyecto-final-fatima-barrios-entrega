param(
    [int]$MaxNumRecords = 8
)

$ErrorActionPreference = "Continue"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$group = "velocity-beam-demo-$timestamp"
$runLog = ".\evidence\demo-directrunner-$timestamp.log"

& .\.venv\Scripts\python.exe -m src.pipeline.streaming_pipeline `
  --bootstrap-servers localhost:9092 `
  --input-topic bank.transactions.raw `
  --alert-topic risk.alerts.velocity `
  --invalid-topic bank.transactions.invalid `
  --consumer-group $group `
  --max-num-records $MaxNumRecords `
  --kafka-environment-type DOCKER `
  --kafka-environment-config apache/beam_java17_sdk:2.75.0 `
  --directrunner-smoke-mode `
  --runner DirectRunner `
  --environment_type LOOPBACK `
  --job_name "velocity-beam-direct-$timestamp" `
  2>&1 | ForEach-Object { $_.ToString() } | Tee-Object -FilePath $runLog

$pipelineExit = $LASTEXITCODE
Write-Output "PIPELINE_EXIT_CODE=$pipelineExit"
Write-Output "CONSUMER_GROUP=$group"
Write-Output "RUN_LOG=$runLog"
exit $pipelineExit
