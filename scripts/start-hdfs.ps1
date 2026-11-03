param([switch]$Stop)

$ErrorActionPreference = 'Stop'
$root = 'D:\kaf\marketplace'
$hadoop = 'D:\tools\hadoop-3.4.1'
$runtime = Join-Path $root 'data\runtime\hadoop'
$utf8 = New-Object System.Text.UTF8Encoding $false

function Stop-Recorded([string]$PidFile) {
    if (-not (Test-Path $PidFile)) { return }
    $procId = [int](Get-Content $PidFile -Raw).Trim()
    cmd.exe /c "taskkill /F /T /PID $procId >nul 2>&1"
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

if ($Stop) {
    Stop-Recorded "$runtime\namenode.pid"
    Stop-Recorded "$runtime\datanode.pid"
    Write-Output 'hdfs stopped'
    return
}

if (-not (Test-Path "$hadoop\bin\hdfs.cmd")) { throw 'Hadoop is not extracted' }
New-Item -ItemType Directory -Force -Path "$hadoop\bin", "$runtime\name", "$runtime\data", "$runtime\tmp", "$root\data\runtime\logs" | Out-Null
$winutils = "$hadoop\bin\winutils.exe"
if (-not (Test-Path $winutils) -or (Get-Item $winutils).Length -lt 50KB) {
    curl.exe -fsSL -o $winutils https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe
    curl.exe -fsSL -o "$hadoop\bin\hadoop.dll" https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll
}
if (-not (Test-Path $winutils) -or (Get-Item $winutils).Length -lt 50KB) { throw 'winutils.exe was not downloaded' }

$env:JAVA_HOME = 'D:\Java\jdk-21'
$env:HADOOP_HOME = $hadoop
$env:HADOOP_CONF_DIR = "$root\hadoop\host"
$env:HADOOP_USER_NAME = 'root'
$env:HADOOP_OPTS = '--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED'
$env:Path = "$hadoop\bin;$env:JAVA_HOME\bin;$env:Path"

if (-not (Test-Path "$runtime\name\current\VERSION")) {
    & "$hadoop\bin\hdfs.cmd" namenode -format -force -nonInteractive *> "$root\data\runtime\logs\hdfs-format.log"
    if ($LASTEXITCODE -ne 0) { throw 'namenode format failed' }
}

$env:HADOOP_HEAPSIZE_MAX = '512m'
$nn = Start-Process -FilePath "$hadoop\bin\hdfs.cmd" -ArgumentList 'namenode' -WindowStyle Hidden -PassThru -RedirectStandardOutput "$root\data\runtime\logs\namenode.out" -RedirectStandardError "$root\data\runtime\logs\namenode.err"
[System.IO.File]::WriteAllText("$runtime\namenode.pid", "$($nn.Id)`n", $utf8)
$env:HADOOP_HEAPSIZE_MAX = '256m'
$dn = Start-Process -FilePath "$hadoop\bin\hdfs.cmd" -ArgumentList 'datanode' -WindowStyle Hidden -PassThru -RedirectStandardOutput "$root\data\runtime\logs\datanode.out" -RedirectStandardError "$root\data\runtime\logs\datanode.err"
[System.IO.File]::WriteAllText("$runtime\datanode.pid", "$($dn.Id)`n", $utf8)

$ready = $false
for ($i = 0; $i -lt 40; $i++) {
    try {
        $response = Invoke-WebRequest -UseBasicParsing 'http://localhost:9870/webhdfs/v1/?op=LISTSTATUS&user.name=root' -TimeoutSec 5
        if ($response.StatusCode -eq 200) { $ready = $true; break }
    } catch {
        Start-Sleep 3
    }
}
if (-not $ready) { throw 'HDFS Web UI did not open :9870' }
Write-Output 'hdfs is listening'
