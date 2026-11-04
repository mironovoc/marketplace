$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\env.ps1"
Set-Location $root
Ensure-Java
$tools = Require-Tools
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function Wait-File([string]$Path, [int]$Seconds) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-Path $Path) {
            if ((Get-Item $Path).Length -gt 0) { return }
        }
        Start-Sleep 2
    }
    throw "file is not ready: $Path"
}

function Write-LfFile([string]$Path) {
    $text = [System.IO.File]::ReadAllText($Path).Replace("`r`n", "`n")
    if (-not $text.EndsWith("`n")) { $text += "`n" }
    [System.IO.File]::WriteAllText($Path, $text)
}

New-Item -ItemType Directory -Force -Path "$root\data\runtime", "$root\evidence", "$root\connect-plugins" | Out-Null

function Assert-Size([string]$Path, [long]$MinBytes) {
    $length = (Get-Item $Path).Length
    if ($length -lt $MinBytes) { throw "archive is incomplete: $Path ($length bytes)" }
}
if (-not (Test-Path (Join-Path $tools 'kafka_2.13-3.7.1'))) {
    Assert-Size (Join-Path $tools 'downloads\kafka.tgz') 100MB
    tar -xzf (Join-Path $tools 'downloads\kafka.tgz') -C $tools
}
if (-not (Test-Path (Join-Path $tools 'hadoop-3.4.1'))) {
    Assert-Size (Join-Path $tools 'downloads\hadoop.tgz') 700MB
    tar -xzf (Join-Path $tools 'downloads\hadoop.tgz') -C $tools
}
if (-not (Test-Path (Join-Path $tools 'spark-3.5.3-bin-hadoop3'))) {
    Assert-Size (Join-Path $tools 'downloads\spark.tgz') 300MB
    tar -xzf (Join-Path $tools 'downloads\spark.tgz') -C $tools
}
if (-not (Test-Path (Join-Path $tools 'prometheus-2.54.1.windows-amd64\prometheus.exe'))) {
    Assert-Size (Join-Path $tools 'downloads\prometheus.zip') 20MB
    Expand-Archive -Force (Join-Path $tools 'downloads\prometheus.zip') $tools
}
if (-not (Test-Path (Join-Path $tools 'alertmanager-0.27.0.windows-amd64\alertmanager.exe'))) {
    Assert-Size (Join-Path $tools 'downloads\alertmanager.zip') 10MB
    Expand-Archive -Force (Join-Path $tools 'downloads\alertmanager.zip') $tools
}
$grafanaExe = Get-ChildItem -Path $tools -Directory -Filter 'grafana*' -ErrorAction SilentlyContinue |
    ForEach-Object { Get-ChildItem (Join-Path $_.FullName 'bin') -Filter 'grafana*.exe' -ErrorAction SilentlyContinue } |
    Select-Object -First 1
if (-not $grafanaExe) {
    Assert-Size (Join-Path $tools 'downloads\grafana.zip') 50MB
    Expand-Archive -Force (Join-Path $tools 'downloads\grafana.zip') $tools
}

& "$root\scripts\generate-certs.ps1"
Write-LfFile "$root\scripts\init-cluster.sh"
Write-LfFile "$root\hadoop\entrypoint.sh"

& (Get-GradleBat) -p "$root\app" shadowJar
if ($LASTEXITCODE -ne 0) { throw 'gradle build failed' }

New-Item -ItemType Directory -Force -Path "$root\spark\classes" | Out-Null
$sparkHome = Get-SparkHome
& javac --release 17 -encoding UTF-8 -cp "$sparkHome\jars\*" -d "$root\spark\classes" "$root\spark\SparkRecommendations.java"
if ($LASTEXITCODE -ne 0) { throw 'spark job did not compile' }
if (Test-Path "$root\spark\recommendations.jar") { Remove-Item "$root\spark\recommendations.jar" }
& jar cf "$root\spark\recommendations.jar" -C "$root\spark\classes" .

$kafkaHome = Get-KafkaHome
Copy-Item "$kafkaHome\libs\connect-file-*.jar" "$root\connect-plugins" -Force
Copy-Item "$kafkaHome\libs\connect-mirror-*.jar" "$root\connect-plugins" -Force
$jmxJar = "$root\monitoring\jmx_prometheus_javaagent.jar"
if (-not (Test-Path $jmxJar) -or (Get-Item $jmxJar).Length -lt 100KB) {
    curl.exe -fsSL -o $jmxJar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar
}

& "$root\scripts\down.ps1"
& "$root\scripts\start-brokers.ps1"
& "$root\scripts\init-cluster.ps1"
& "$root\scripts\start-hdfs.ps1"
& "$root\scripts\flow.ps1"
