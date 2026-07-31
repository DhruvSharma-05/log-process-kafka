<#
    Downloads the sample datasets used by the pipeline into data/.
    Idempotent: already-downloaded files are skipped.

        powershell -ExecutionPolicy Bypass -File scripts\download_data.ps1

    Sources:
      NASA-HTTP  https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html  (~205 MB uncompressed)
      loghub     https://github.com/logpai/loghub                    (2k-line samples)
#>
[CmdletBinding()]
param(
    [switch]$SkipNasa   # loghub samples only (fast)
)

$ErrorActionPreference = 'Stop'
$DataDir = Join-Path (Split-Path $PSScriptRoot -Parent) 'data'
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null

function Get-RemoteFile {
    param([string]$Url, [string]$Destination)

    if (Test-Path $Destination) {
        $mb = [math]::Round((Get-Item $Destination).Length / 1MB, 1)
        Write-Host ("  skip  {0} ({1} MB, already present)" -f (Split-Path $Destination -Leaf), $mb)
        return
    }
    Write-Host ("  get   {0}" -f (Split-Path $Destination -Leaf))
    $previous = $ProgressPreference
    $ProgressPreference = 'SilentlyContinue'   # progress bar makes this ~10x slower
    try {
        Invoke-WebRequest -Uri $Url -OutFile $Destination -UseBasicParsing -TimeoutSec 600
    } finally {
        $ProgressPreference = $previous
    }
}

function Expand-Gzip {
    param([string]$Source, [string]$Destination)

    if (Test-Path $Destination) {
        $mb = [math]::Round((Get-Item $Destination).Length / 1MB, 1)
        Write-Host ("  skip  {0} ({1} MB, already extracted)" -f (Split-Path $Destination -Leaf), $mb)
        return
    }
    Write-Host ("  unzip {0}" -f (Split-Path $Destination -Leaf))
    $in = [System.IO.File]::OpenRead($Source)
    try {
        $gz = New-Object System.IO.Compression.GZipStream($in, [System.IO.Compression.CompressionMode]::Decompress)
        try {
            $out = [System.IO.File]::Create($Destination)
            try { $gz.CopyTo($out) } finally { $out.Dispose() }
        } finally { $gz.Dispose() }
    } finally { $in.Dispose() }
}

# --- NASA Kennedy Space Center HTTP logs, August 1995 -------------------------
# 1.57M real requests in Common Log Format. Our realistic ingestion baseline.
if (-not $SkipNasa) {
    Write-Host "`nNASA-HTTP access logs (Aug 1995)"
    $gz  = Join-Path $DataDir 'NASA_access_log_Aug95.gz'
    $raw = Join-Path $DataDir 'NASA_access_log_Aug95'
    Get-RemoteFile -Url 'https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz' -Destination $gz
    Expand-Gzip -Source $gz -Destination $raw
}

# --- loghub samples ----------------------------------------------------------
# Deliberately heterogeneous: these are NOT access logs. They exist to prove the
# processor routes unparseable input to the DLQ instead of crashing (M2).
Write-Host "`nloghub samples (2k lines each)"
$LogHub = Join-Path $DataDir 'loghub'
New-Item -ItemType Directory -Force -Path $LogHub | Out-Null

$samples = @{
    'Apache_2k.log'  = 'https://raw.githubusercontent.com/logpai/loghub/master/Apache/Apache_2k.log'
    'OpenSSH_2k.log' = 'https://raw.githubusercontent.com/logpai/loghub/master/OpenSSH/OpenSSH_2k.log'
    'HDFS_2k.log'    = 'https://raw.githubusercontent.com/logpai/loghub/master/HDFS/HDFS_2k.log'
    'Linux_2k.log'   = 'https://raw.githubusercontent.com/logpai/loghub/master/Linux/Linux_2k.log'
}
foreach ($name in $samples.Keys) {
    Get-RemoteFile -Url $samples[$name] -Destination (Join-Path $LogHub $name)
}

Write-Host "`nContents of data/:"
Get-ChildItem -Recurse -File $DataDir |
    Select-Object @{n = 'Size(MB)'; e = { [math]::Round($_.Length / 1MB, 2) } },
                  @{n = 'Path'; e = { $_.FullName.Substring($DataDir.Length + 1) } } |
    Format-Table -AutoSize

Write-Host "Done. Next: python producer\main.py --file data\NASA_access_log_Aug95 --rate 2000"
