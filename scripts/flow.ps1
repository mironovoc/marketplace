$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\env.ps1"
Ensure-Java
Set-Location $root
$generated = Initialize-GeneratedConfigs
$kafkaHome = Get-KafkaHome
$kafkaBin = Join-Path $kafkaHome 'bin\windows'

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

$jar = "$root\app\build\libs\marketplace.jar"
$grafanaExe = Get-GrafanaExe
$env:GF_PATHS_PROVISIONING = Join-Path $generated 'provisioning'
Start-Process java -ArgumentList '-Dfile.encoding=UTF-8','-jar',$jar,'alerts' -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput "$root\evidence\alerts-server.log" -RedirectStandardError "$root\evidence\alerts-server.err"
Start-Process (Get-PrometheusExe) -ArgumentList "--config.file=$root\monitoring\prometheus.yml","--web.listen-address=:9090","--storage.tsdb.path=$root\data\runtime\prometheus" -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput "$root\evidence\prometheus.log" -RedirectStandardError "$root\evidence\prometheus.err"
Start-Process (Get-AlertmanagerExe) -ArgumentList "--config.file=$root\monitoring\alertmanager.yml","--web.listen-address=:9093","--storage.path=$root\data\runtime\alertmanager" -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput "$root\evidence\alertmanager.log" -RedirectStandardError "$root\evidence\alertmanager.err"
$grafanaHome = $grafanaExe.Directory.Parent.FullName
Start-Process $grafanaExe.FullName -ArgumentList "--config=$root\grafana\grafana.ini","--homepath=$grafanaHome" -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput "$root\evidence\grafana.log" -RedirectStandardError "$root\evidence\grafana.err"

$env:KAFKA_HEAP_OPTS = '-Xmx1024M'
$env:LOG_DIR = "$root\data\runtime\connect-logs"
New-Item -ItemType Directory -Force -Path $env:LOG_DIR | Out-Null
Start-Process "$env:ComSpec" -ArgumentList '/c',"$kafkaBin\connect-distributed.bat","$generated\connect-distributed.properties" -WorkingDirectory $root -WindowStyle Hidden

$connectReady = $false
for ($i = 0; $i -lt 40; $i++) {
    try {
        Invoke-RestMethod http://localhost:8083/connectors | Out-Null
        $connectReady = $true
        break
    } catch {
        Start-Sleep 3
    }
}
if (-not $connectReady) { throw 'Kafka Connect did not open :8083' }

foreach ($config in @('file-sink-products.json','file-sink-searches.json')) {
    try {
        Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors -ContentType 'application/json' -Body (Get-Content "$generated\$config" -Raw) | Out-Null
    } catch {
        $code = 0
        if ($_.Exception.Response) { $code = [int]$_.Exception.Response.StatusCode }
        if ($code -ne 409) { throw }
    }
}
Write-Output 'connectors are running'
Start-Process "$env:ComSpec" -ArgumentList '/c',"$kafkaBin\kafka-run-class.bat",'org.apache.kafka.connect.mirror.MirrorMaker',"$generated\mm2.properties" -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput "$root\data\runtime\logs\mm2.out" -RedirectStandardError "$root\data\runtime\logs\mm2.err"

Start-Process java -ArgumentList '-Dfile.encoding=UTF-8','-jar',$jar,'filter' -WorkingDirectory $root -WindowStyle Hidden -RedirectStandardOutput "$root\evidence\filter.log" -RedirectStandardError "$root\evidence\filter.err"
Wait-File "$root\evidence\filter.log" 40
Start-Sleep 12

& java "-Dfile.encoding=UTF-8" -jar $jar ban add 99999 | Tee-Object -FilePath "$root\evidence\ban.txt"
Start-Sleep 8
& java "-Dfile.encoding=UTF-8" -jar $jar shop | Tee-Object -FilePath "$root\evidence\shop.txt"
& java "-Dfile.encoding=UTF-8" -jar $jar prove-acl | Tee-Object -FilePath "$root\evidence\acl-deny.txt"

$clean = "$root\data\runtime\products-clean.jsonl"
$filtered = $false
for ($i = 0; $i -lt 30; $i++) {
    if (Test-Path $clean) {
        $text = Get-Content $clean -Raw
        if ($text -match '12345' -and $text -notmatch '99999') {
            $filtered = $true
            break
        }
    }
    Start-Sleep 3
}
if (-not $filtered) { throw 'filtered file does not contain the allowed product or still contains the banned one' }
Copy-Item $clean "$root\evidence\products-clean.jsonl"
Write-Output 'filtered storage file is ready'

$env:MARKETPLACE_HOME = $root
$env:BOOTSTRAP_A = 'localhost:19092,localhost:19094,localhost:19096'
$env:BOOTSTRAP_B = 'localhost:19192'
$env:WEBHDFS = 'http://localhost:9870/webhdfs/v1'
$env:HDFS_URI = 'hdfs://localhost:9000'
$env:HADOOP_HOME = Get-HadoopHome
$env:HADOOP_CONF_DIR = "$root\data\runtime\hadoop-conf"
$env:HADOOP_USER_NAME = 'root'
$env:SPARK_LOCAL_DIRS = "$root\data\runtime\spark"
$env:HADOOP_OPTS = '--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED'
$env:SPARK_SUBMIT_OPTS = $env:HADOOP_OPTS
$env:Path = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:Path"
& java "-Dfile.encoding=UTF-8" -jar $jar lake *> "$root\evidence\lake.txt"
if ($LASTEXITCODE -ne 0) { throw 'lake loader failed' }
$sparkSubmit = Join-Path (Get-SparkHome) 'bin\spark-submit.cmd'
cmd.exe /c "`"$sparkSubmit`" --master local[1] --driver-memory 512m --class org.marketplace.SparkRecommendations `"$root\spark\recommendations.jar`" > `"$root\evidence\spark.txt`" 2>&1"
if ($LASTEXITCODE -ne 0) { throw 'spark job failed' }
& java "-Dfile.encoding=UTF-8" -jar $jar publish *> "$root\evidence\recommendations.txt"
if ($LASTEXITCODE -ne 0) { throw 'recommendation publisher failed' }

$query = -join ([char]0x0447, [char]0x0430, [char]0x0441, [char]0x044B)
& java "-Dfile.encoding=UTF-8" -jar $jar client search $query | Tee-Object -FilePath "$root\evidence\search.txt"
Start-Sleep 5
if (Test-Path "$root\data\runtime\searches.jsonl") {
    Copy-Item "$root\data\runtime\searches.jsonl" "$root\evidence\searches.jsonl"
}
& java "-Dfile.encoding=UTF-8" -jar $jar client recommend | Tee-Object -FilePath "$root\evidence\client-recommendations.txt"
& java "-Dfile.encoding=UTF-8" -jar $jar ban list | Tee-Object -FilePath "$root\evidence\ban-list.txt" -Append

cmd.exe /c "`"$kafkaBin\kafka-topics.bat`" --bootstrap-server localhost:19192 --command-config `"$root\data\runtime\admin-host.properties`" --describe > `"$root\evidence\cluster-b-topics.txt`" 2>&1"

& "$root\scripts\start-brokers.ps1" -Stop -Name kafka-a3
Start-Sleep 50
curl.exe -s http://localhost:9093/api/v2/alerts -o "$root\evidence\alertmanager-alerts.json"
curl.exe -s "http://localhost:9090/api/v1/query?query=up" -o "$root\evidence\prometheus-up.json"
& "$root\scripts\start-brokers.ps1" -Name kafka-a3
Write-Output 'demo finished'
