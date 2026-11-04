$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\env.ps1"
Ensure-Java
$openssl = Find-OpenSsl
$keytool = Join-Path $env:JAVA_HOME 'bin\keytool.exe'
$pass = 'your-password'
$secrets = Join-Path $root 'secrets'

if (-not (Test-Path $openssl)) { throw "openssl not found: $openssl" }
if (-not (Test-Path $keytool)) { throw "keytool not found: $keytool" }

if (Test-Path $secrets) { Remove-Item $secrets -Recurse -Force }
New-Item -ItemType Directory -Path (Join-Path $secrets 'ca'), (Join-Path $secrets 'clients') | Out-Null

function Write-Lf([string]$Path, [string]$Text) {
    $normalized = $Text.Replace("`r`n", "`n").TrimEnd("`n") + "`n"
    [System.IO.File]::WriteAllText($Path, $normalized)
}

& $openssl req -new -x509 -days 3650 -nodes `
    -keyout (Join-Path $secrets 'ca\ca.key') `
    -out (Join-Path $secrets 'ca\ca.crt') `
    -subj '/CN=marketplace-kafka-ca'

function New-Cert([string]$Name, [string]$Dir, [string]$Sans) {
    New-Item -ItemType Directory -Force -Path $Dir | Out-Null
    $cnf = Join-Path $Dir "$Name.cnf"
    Write-Lf $cnf @"
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no
[req_distinguished_name]
CN = $Name
[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = $Sans
"@
    & $openssl req -new -nodes -newkey rsa:2048 `
        -keyout (Join-Path $Dir "$Name.key") `
        -out (Join-Path $Dir "$Name.csr") `
        -config $cnf
    & $openssl x509 -req -days 3650 `
        -in (Join-Path $Dir "$Name.csr") `
        -CA (Join-Path $secrets 'ca\ca.crt') `
        -CAkey (Join-Path $secrets 'ca\ca.key') `
        -CAcreateserial `
        -out (Join-Path $Dir "$Name.crt") `
        -extfile $cnf -extensions v3_req
    & $openssl pkcs12 -export `
        -in (Join-Path $Dir "$Name.crt") `
        -inkey (Join-Path $Dir "$Name.key") `
        -chain -CAfile (Join-Path $secrets 'ca\ca.crt') `
        -name $Name `
        -out (Join-Path $Dir "$Name.keystore.p12") `
        -passout "pass:$pass"
}

function New-Trust([string]$Dir) {
    & $keytool -importcert -noprompt -alias ca `
        -file (Join-Path $secrets 'ca\ca.crt') `
        -keystore (Join-Path $Dir 'truststore.jks') `
        -storepass $pass | Out-Null
    [System.IO.File]::WriteAllText((Join-Path $Dir 'keystore_creds'), "$pass`n")
    [System.IO.File]::WriteAllText((Join-Path $Dir 'key_creds'), "$pass`n")
    [System.IO.File]::WriteAllText((Join-Path $Dir 'truststore_creds'), "$pass`n")
}

foreach ($broker in @('kafka-a1', 'kafka-a2', 'kafka-a3', 'kafka-b1')) {
    $dir = Join-Path $secrets $broker
    New-Cert $broker $dir "DNS:$broker,DNS:localhost,IP:127.0.0.1"
    New-Trust $dir
    Write-Lf (Join-Path $dir 'client.properties') @"
security.protocol=SSL
ssl.keystore.location=/etc/kafka/secrets/$broker.keystore.p12
ssl.keystore.password=$pass
ssl.keystore.type=PKCS12
ssl.key.password=$pass
ssl.truststore.location=/etc/kafka/secrets/truststore.jks
ssl.truststore.password=$pass
ssl.truststore.type=JKS
ssl.endpoint.identification.algorithm=
"@
}

$clientDir = Join-Path $secrets 'clients'
foreach ($user in @('admin', 'shop', 'client', 'streams', 'connect', 'lake', 'spark')) {
    New-Cert $user $clientDir 'DNS:localhost,IP:127.0.0.1'
}
New-Trust $clientDir
Write-Lf (Join-Path $clientDir 'admin.properties') @"
security.protocol=SSL
ssl.keystore.location=/etc/kafka/secrets/admin.keystore.p12
ssl.keystore.password=$pass
ssl.keystore.type=PKCS12
ssl.key.password=$pass
ssl.truststore.location=/etc/kafka/secrets/truststore.jks
ssl.truststore.password=$pass
ssl.truststore.type=JKS
ssl.endpoint.identification.algorithm=
"@

Write-Output "certificates are in $secrets"
