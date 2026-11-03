param(
    [string]$Name,
    [switch]$Stop
)

$ErrorActionPreference = 'Stop'
$root = 'D:\kaf\marketplace'
$kafkaHome = 'D:\tools\kafka_2.13-3.7.1'
$bin = Join-Path $kafkaHome 'bin\windows'
$runtime = Join-Path $root 'data\runtime'
$utf8 = New-Object System.Text.UTF8Encoding $false

$brokers = @(
    @{ Name = 'kafka-a1'; NodeId = 1; Port = 19092; Controller = 29093; Jmx = 17071; Cluster = 'A' }
    @{ Name = 'kafka-a2'; NodeId = 2; Port = 19094; Controller = 29095; Jmx = 17072; Cluster = 'A' }
    @{ Name = 'kafka-a3'; NodeId = 3; Port = 19096; Controller = 29097; Jmx = 17073; Cluster = 'A' }
    @{ Name = 'kafka-b1'; NodeId = 1; Port = 19192; Controller = 29099; Jmx = 17074; Cluster = 'B' }
)

function Write-Text([string]$Path, [string]$Text) {
    $dir = Split-Path $Path
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    [System.IO.File]::WriteAllText($Path, $Text, $utf8)
}

function Stop-Broker([string]$BrokerName) {
    $pidFile = Join-Path $runtime "brokers\$BrokerName.pid"
    if (-not (Test-Path $pidFile)) { return }
    $procId = [int](Get-Content $pidFile -Raw).Trim()
    cmd.exe /c "taskkill /F /T /PID $procId >nul 2>&1"
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
}

if (-not $Stop -and -not $Name) {
    Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" | Where-Object {
        $_.CommandLine -match 'data\\runtime\\brokers\\kafka-'
    } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}

if ($Stop) {
    $targets = if ($Name) { @($Name) } else { $brokers.Name }
    foreach ($brokerName in $targets) { Stop-Broker $brokerName }
    if (-not $Name) { Write-Output 'brokers stopped' }
    return
}

$selected = if ($Name) { @($brokers | Where-Object Name -eq $Name) } else { $brokers }
if ($selected.Count -eq 0) { throw "unknown broker $Name" }

$admin = @"
security.protocol=SSL
ssl.keystore.location=D:/kaf/marketplace/secrets/clients/admin.keystore.p12
ssl.keystore.password=your-password
ssl.keystore.type=PKCS12
ssl.key.password=your-password
ssl.truststore.location=D:/kaf/marketplace/secrets/clients/truststore.jks
ssl.truststore.password=your-password
ssl.truststore.type=JKS
ssl.endpoint.identification.algorithm=
"@
Write-Text (Join-Path $runtime 'admin-host.properties') $admin

foreach ($broker in $selected) {
    $isA = $broker.Cluster -eq 'A'
    $voters = if ($isA) { '1@localhost:29093,2@localhost:29095,3@localhost:29097' } else { '1@localhost:29099' }
    $clusterId = if ($isA) { 'MkU3OEVBNTcwNTJENDM2Qk' } else { 'bK7uY2xQ0nR4sT8vW1aZ3c' }
    $superUsers = if ($isA) {
        'User:admin;User:kafka-a1;User:kafka-a2;User:kafka-a3;User:connect;User:streams'
    } else {
        'User:admin;User:kafka-b1;User:connect;User:lake;User:spark'
    }
    $rf = if ($isA) { 3 } else { 1 }
    $isr = if ($isA) { 2 } else { 1 }
    $config = Join-Path $runtime "brokers\$($broker.Name).properties"
    $logDir = "D:/kaf/marketplace/data/runtime/kafka/$($broker.Name)"
    $text = @"
process.roles=broker,controller
node.id=$($broker.NodeId)
controller.quorum.voters=$voters
listeners=SSL://0.0.0.0:$($broker.Port),CONTROLLER://0.0.0.0:$($broker.Controller)
advertised.listeners=SSL://localhost:$($broker.Port)
listener.security.protocol.map=CONTROLLER:SSL,SSL:SSL
controller.listener.names=CONTROLLER
inter.broker.listener.name=SSL
log.dirs=$logDir
ssl.keystore.location=D:/kaf/marketplace/secrets/$($broker.Name)/$($broker.Name).keystore.p12
ssl.keystore.password=your-password
ssl.key.password=your-password
ssl.keystore.type=PKCS12
ssl.truststore.location=D:/kaf/marketplace/secrets/$($broker.Name)/truststore.jks
ssl.truststore.password=your-password
ssl.truststore.type=JKS
ssl.client.auth=required
ssl.endpoint.identification.algorithm=
ssl.principal.mapping.rules=RULE:^CN=([^,]*).*/`$1/
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
allow.everyone.if.no.acl.found=false
super.users=$superUsers
num.partitions=$rf
default.replication.factor=$rf
min.insync.replicas=$isr
offsets.topic.replication.factor=$rf
transaction.state.log.replication.factor=$rf
transaction.state.log.min.isr=$isr
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
unclean.leader.election.enable=false
log.retention.hours=168
log.segment.bytes=104857600
"@
    Write-Text $config $text
    New-Item -ItemType Directory -Force -Path ($logDir.Replace('/', '\')) | Out-Null
    $meta = Join-Path ($logDir.Replace('/', '\')) 'meta.properties'
    if (-not (Test-Path $meta)) {
        & "$bin\kafka-storage.bat" format -t $clusterId -c $config --ignore-formatted | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "storage format failed for $($broker.Name)" }
    }
    $outLog = Join-Path $runtime "logs\$($broker.Name).out"
    $errLog = Join-Path $runtime "logs\$($broker.Name).err"
    New-Item -ItemType Directory -Force -Path (Split-Path $outLog) | Out-Null
    $env:JAVA_HOME = 'D:\Java\jdk-21'
    $env:KAFKA_HEAP_OPTS = '-Xmx384M -Xms128M'
    $env:KAFKA_LOG4J_OPTS = '-Dlog4j.configuration=file:///D:/kaf/marketplace/config/kafka-log4j.properties'
    $env:KAFKA_OPTS = "-javaagent:D:/kaf/marketplace/monitoring/jmx_prometheus_javaagent.jar=$($broker.Jmx):D:/kaf/marketplace/monitoring/kafka-jmx.yml"
    $proc = Start-Process -FilePath "$bin\kafka-server-start.bat" -ArgumentList $config -WorkingDirectory $kafkaHome -WindowStyle Hidden -PassThru -RedirectStandardOutput $outLog -RedirectStandardError $errLog
    Write-Text (Join-Path $runtime "brokers\$($broker.Name).pid") "$($proc.Id)`n"
    Remove-Item Env:KAFKA_OPTS -ErrorAction SilentlyContinue
    Remove-Item Env:KAFKA_LOG4J_OPTS -ErrorAction SilentlyContinue
}

$adminPath = Join-Path $runtime 'admin-host.properties'
foreach ($broker in $selected) {
    $ready = $false
    for ($i = 0; $i -lt 40; $i++) {
        $probe = Join-Path $runtime "logs\$($broker.Name).probe"
        cmd.exe /c "`"$bin\kafka-broker-api-versions.bat`" --bootstrap-server localhost:$($broker.Port) --command-config `"$adminPath`" > `"$probe`" 2>&1"
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep 3
    }
    if (-not $ready) { throw "broker $($broker.Name) did not accept TLS clients" }
}
Write-Output 'brokers are listening'
