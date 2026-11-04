$ErrorActionPreference = 'Continue'
. "$PSScriptRoot\env.ps1"
Set-Location $root
New-Item -ItemType Directory -Force -Path "$root\data\runtime" | Out-Null
Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" | Where-Object {
    $_.CommandLine -match 'marketplace.jar|connect-distributed|kafka.Kafka|org.apache.hadoop.hdfs|MirrorMaker'
} | ForEach-Object {
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
}
Get-Process prometheus, alertmanager, grafana-server, grafana -ErrorAction SilentlyContinue | Stop-Process -Force
if (Test-Path "$root\scripts\start-brokers.ps1") {
    & "$root\scripts\start-brokers.ps1" -Stop
}
if (Test-Path "$root\scripts\start-hdfs.ps1") {
    & "$root\scripts\start-hdfs.ps1" -Stop
}
$docker = Find-Docker
if ($docker) {
    $proc = Start-Process -FilePath $docker -ArgumentList @('compose', '-f', "$root\docker-compose.yml", 'down', '--timeout', '15') -WorkingDirectory $root -WindowStyle Hidden -PassThru -RedirectStandardOutput "$root\data\runtime\docker-down.txt" -RedirectStandardError "$root\data\runtime\docker-down.err"
    if ($proc -and -not $proc.WaitForExit(30000)) {
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    }
}
Write-Output 'stopped'
