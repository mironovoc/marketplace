# Корень проекта: MARKETPLACE_ROOT, иначе каталог на уровень выше scripts.
# Инструменты и JDK: MARKETPLACE_TOOLS и JAVA_HOME.
# Машинные пути этой машины можно положить в scripts/local.ps1 (файл в git не входит).

$localPaths = Join-Path $PSScriptRoot 'local.ps1'
if (Test-Path $localPaths) {
    . $localPaths
}

if ($env:MARKETPLACE_ROOT) {
    $root = (Resolve-Path $env:MARKETPLACE_ROOT).Path
} else {
    $root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$root = $root.TrimEnd('\')
$env:MARKETPLACE_HOME = $root

function Require-Tools {
    if ($env:MARKETPLACE_TOOLS -and (Test-Path $env:MARKETPLACE_TOOLS)) {
        return (Resolve-Path $env:MARKETPLACE_TOOLS).Path.TrimEnd('\')
    }
    throw 'Set MARKETPLACE_TOOLS to the directory with Kafka, Hadoop, Spark, Prometheus, Grafana, Alertmanager and Gradle. See scripts/local.ps1.example.'
}

function Ensure-Java {
    if ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME 'bin\java.exe'))) {
        $env:Path = "$(Join-Path $env:JAVA_HOME 'bin');$env:Path"
        return
    }
    $cmd = Get-Command java.exe -ErrorAction SilentlyContinue
    if ($cmd) {
        $home = Split-Path (Split-Path $cmd.Source)
        if (Test-Path (Join-Path $home 'bin\java.exe')) {
            $env:JAVA_HOME = $home
            $env:Path = "$(Join-Path $home 'bin');$env:Path"
            return
        }
    }
    throw 'Set JAVA_HOME to a JDK 17 or newer. See scripts/local.ps1.example.'
}

function Get-KafkaHome {
    $path = Join-Path (Require-Tools) 'kafka_2.13-3.7.1'
    if (-not (Test-Path (Join-Path $path 'bin\windows\kafka-server-start.bat'))) {
        throw "Kafka 3.7.1 not found: $path"
    }
    return $path
}

function Get-HadoopHome {
    $path = Join-Path (Require-Tools) 'hadoop-3.4.1'
    if (-not (Test-Path (Join-Path $path 'bin\hdfs.cmd'))) {
        throw "Hadoop 3.4.1 not found: $path"
    }
    return $path
}

function Get-SparkHome {
    $path = Join-Path (Require-Tools) 'spark-3.5.3-bin-hadoop3'
    if (-not (Test-Path (Join-Path $path 'bin\spark-submit.cmd'))) {
        throw "Spark 3.5.3 not found: $path"
    }
    return $path
}

function Get-PrometheusExe {
    $path = Join-Path (Require-Tools) 'prometheus-2.54.1.windows-amd64\prometheus.exe'
    if (-not (Test-Path $path)) { throw "Prometheus not found: $path" }
    return $path
}

function Get-AlertmanagerExe {
    $path = Join-Path (Require-Tools) 'alertmanager-0.27.0.windows-amd64\alertmanager.exe'
    if (-not (Test-Path $path)) { throw "Alertmanager not found: $path" }
    return $path
}

function Get-GrafanaExe {
    $tools = Require-Tools
    $dirs = @(Get-ChildItem -Path $tools -Directory -Filter 'grafana*' -ErrorAction SilentlyContinue)
    foreach ($dir in $dirs) {
        foreach ($name in @('grafana-server.exe', 'grafana.exe')) {
            $exe = Join-Path $dir.FullName "bin\$name"
            if (Test-Path $exe) { return (Get-Item $exe) }
        }
    }
    throw "Grafana not found under $tools"
}

function Get-GradleBat {
    $onPath = Get-Command gradle.bat -ErrorAction SilentlyContinue
    if ($onPath) { return $onPath.Source }
    $tools = Require-Tools
    $hit = Get-ChildItem -Path $tools -Filter gradle.bat -Recurse -Depth 3 -ErrorAction SilentlyContinue |
        Where-Object { $_.FullName -match '\\bin\\gradle\.bat$' } |
        Select-Object -First 1
    if ($hit) { return $hit.FullName }
    throw "Gradle not found on PATH or under $tools"
}

function Find-Docker {
    $cmd = Get-Command docker.exe -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    foreach ($candidate in @(
            (Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe'),
            (Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe')
        )) {
        if ($candidate -and (Test-Path $candidate)) { return $candidate }
    }
    return $null
}

function Find-OpenSsl {
    $cmd = Get-Command openssl.exe -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    foreach ($candidate in @(
            (Join-Path $env:ProgramFiles 'Git\usr\bin\openssl.exe'),
            (Join-Path ${env:ProgramFiles(x86)} 'Git\usr\bin\openssl.exe')
        )) {
        if ($candidate -and (Test-Path $candidate)) { return $candidate }
    }
    throw 'openssl.exe not found. Install Git for Windows or put openssl on PATH.'
}

function Expand-MarketplaceTemplate([string]$Name, [string]$Destination) {
    $template = Join-Path $root "config\templates\$Name"
    if (-not (Test-Path $template)) { throw "template not found: $template" }
    $text = [System.IO.File]::ReadAllText($template)
    $text = $text.Replace('{{ROOT}}', ($root.Replace('\', '/')))
    if ($text.Contains('{{')) { throw "unresolved placeholder in $Name" }
    $dir = Split-Path $Destination
    if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
    $utf8 = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllText($Destination, ($text.Replace("`r`n", "`n")), $utf8)
}

function Initialize-GeneratedConfigs {
    $gen = Join-Path $root 'data\runtime\generated'
    New-Item -ItemType Directory -Force -Path $gen | Out-Null
    Expand-MarketplaceTemplate 'connect-distributed.properties' (Join-Path $gen 'connect-distributed.properties')
    Expand-MarketplaceTemplate 'mm2.properties' (Join-Path $gen 'mm2.properties')
    Expand-MarketplaceTemplate 'file-sink-products.json' (Join-Path $gen 'file-sink-products.json')
    Expand-MarketplaceTemplate 'file-sink-searches.json' (Join-Path $gen 'file-sink-searches.json')
    Expand-MarketplaceTemplate 'mirror.json' (Join-Path $gen 'mirror.json')

    $prov = Join-Path $gen 'provisioning'
    New-Item -ItemType Directory -Force -Path (Join-Path $prov 'datasources'), (Join-Path $prov 'dashboards') | Out-Null
    Copy-Item (Join-Path $root 'grafana\provisioning\datasources\datasource.yml') (Join-Path $prov 'datasources\datasource.yml') -Force
    Expand-MarketplaceTemplate 'grafana-dashboards.yml' (Join-Path $prov 'dashboards\dashboards.yml')

    $hadoopConf = Join-Path $root 'data\runtime\hadoop-conf'
    Expand-MarketplaceTemplate 'core-site.xml' (Join-Path $hadoopConf 'core-site.xml')
    Expand-MarketplaceTemplate 'hdfs-site.xml' (Join-Path $hadoopConf 'hdfs-site.xml')
    return $gen
}
