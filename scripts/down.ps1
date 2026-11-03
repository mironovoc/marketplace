$ErrorActionPreference = 'Continue'
Set-Location D:\kaf\marketplace
New-Item -ItemType Directory -Force -Path 'D:\kaf\marketplace\data\runtime' | Out-Null
Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" | Where-Object {
    $_.CommandLine -match 'marketplace.jar|connect-distributed|kafka.Kafka|org.apache.hadoop.hdfs|MirrorMaker'
} | ForEach-Object {
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
}
Get-Process prometheus, alertmanager, grafana-server, grafana -ErrorAction SilentlyContinue | Stop-Process -Force
if (Test-Path 'D:\kaf\marketplace\scripts\start-brokers.ps1') {
    & 'D:\kaf\marketplace\scripts\start-brokers.ps1' -Stop
}
if (Test-Path 'D:\kaf\marketplace\scripts\start-hdfs.ps1') {
    & 'D:\kaf\marketplace\scripts\start-hdfs.ps1' -Stop
}
$docker = 'C:\Users\relotech\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe'
if (Test-Path $docker) {
    $proc = Start-Process -FilePath $docker -ArgumentList @('compose', '-f', 'D:\kaf\marketplace\docker-compose.yml', 'down', '--timeout', '15') -WorkingDirectory 'D:\kaf\marketplace' -WindowStyle Hidden -PassThru -RedirectStandardOutput 'D:\kaf\marketplace\data\runtime\docker-down.txt' -RedirectStandardError 'D:\kaf\marketplace\data\runtime\docker-down.err'
    if ($proc -and -not $proc.WaitForExit(30000)) {
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    }
}
Write-Output 'stopped'
