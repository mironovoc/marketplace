$ErrorActionPreference = 'Stop'
$root = 'D:\kaf\marketplace'
$bin = 'D:\tools\kafka_2.13-3.7.1\bin\windows'
$admin = "$root\data\runtime\admin-host.properties"
$env:JAVA_HOME = 'D:\Java\jdk-21'

function Invoke-Kafka([string]$Tool, [string[]]$ArgumentList) {
    & "$bin\$Tool" @ArgumentList
    if ($LASTEXITCODE -ne 0) { throw "$Tool exited $LASTEXITCODE" }
}

$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    & "$bin\kafka-topics.bat" --bootstrap-server localhost:19092 --command-config $admin --list *> "$root\data\runtime\logs\topics-wait.txt"
    if ($LASTEXITCODE -eq 0) { $ready = $true; break }
    Start-Sleep 3
}
if (-not $ready) { throw 'cluster A did not answer kafka-topics' }

function New-Topic([string]$Topic, [int]$Partitions, [string]$Policy, [string[]]$Extra) {
    $topicArgs = @(
        '--bootstrap-server', 'localhost:19092,localhost:19094,localhost:19096',
        '--command-config', $admin,
        '--create', '--if-not-exists',
        '--topic', $Topic,
        '--partitions', "$Partitions",
        '--replication-factor', '3',
        '--config', 'min.insync.replicas=2',
        '--config', "cleanup.policy=$Policy"
    ) + $Extra
    Invoke-Kafka 'kafka-topics.bat' $topicArgs
}

New-Topic 'shop.products' 3 'delete' @('--config', 'retention.ms=604800000', '--config', 'segment.bytes=1073741824')
New-Topic 'shop.products.clean' 3 'delete' @()
New-Topic 'shop.banned' 3 'compact' @()
New-Topic 'client.searches' 3 'delete' @()
New-Topic 'client.recommendation-requests' 3 'delete' @()
New-Topic 'recommendations' 3 'delete' @()
New-Topic 'connect-configs' 1 'compact' @()
New-Topic 'connect-offsets' 3 'compact' @()
New-Topic 'connect-status' 3 'compact' @()

function Add-Acl([string[]]$ArgumentList) {
    Invoke-Kafka 'kafka-acls.bat' (@('--bootstrap-server', 'localhost:19092', '--command-config', $admin, '--add') + $ArgumentList)
}

Add-Acl @('--allow-principal', 'User:shop', '--operation', 'Write', '--operation', 'Describe', '--topic', 'shop.products')
Add-Acl @('--deny-principal', 'User:shop', '--operation', 'Write', '--topic', 'shop.products.clean')
Add-Acl @('--deny-principal', 'User:shop', '--operation', 'Write', '--topic', 'recommendations')
Add-Acl @('--allow-principal', 'User:client', '--operation', 'Write', '--operation', 'Describe', '--topic', 'client.searches')
Add-Acl @('--allow-principal', 'User:client', '--operation', 'Write', '--operation', 'Describe', '--topic', 'client.recommendation-requests')
Add-Acl @('--allow-principal', 'User:client', '--operation', 'Read', '--operation', 'Describe', '--topic', 'recommendations')
Add-Acl @('--allow-principal', 'User:client', '--operation', 'Read', '--operation', 'Describe', '--group', 'client-recommendations')
Add-Acl @('--deny-principal', 'User:client', '--operation', 'Write', '--topic', 'shop.products')
Add-Acl @('--allow-principal', 'User:spark', '--operation', 'Write', '--operation', 'Describe', '--topic', 'recommendations')

& "$bin\kafka-topics.bat" --bootstrap-server localhost:19092 --command-config $admin --describe > "$root\evidence\topics.txt"
& "$bin\kafka-acls.bat" --bootstrap-server localhost:19092 --command-config $admin --list > "$root\evidence\acls.txt"
Add-Content "$root\evidence\topics.txt" 'cluster B answers'
& "$bin\kafka-topics.bat" --bootstrap-server localhost:19192 --command-config $admin --list >> "$root\evidence\topics.txt"
Write-Output 'topics and ACLs are created'
