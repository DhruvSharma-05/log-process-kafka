<#
    M6 load test — the PRD's Section 1.5 success metrics, measured.

        powershell -ExecutionPolicy Bypass -File scripts\load_test.ps1

    Runs the burst scenario the PRD actually specifies: steady baseline load,
    then 2x for a bounded window, with the processor running throughout. Then
    measures how long consumer lag takes to return to zero.

    Target: "Consumer lag recovery time after burst < 60s to drain a 2x spike".

    This is deliberately NOT the M5 test. That one stopped the consumer
    entirely for three minutes, which is a worst-case outage, not a spike.

    The processor must already be running:  make process
#>
[CmdletBinding()]
param(
    [int]$BaselineRate = 1000,
    [int]$BaselineSeconds = 45,
    [int]$SpikeMultiplier = 2,
    [int]$SpikeSeconds = 60,
    [int]$MaxDrainSeconds = 300
)

$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path $PSScriptRoot -Parent
Set-Location $RepoRoot

function Get-Lag {
    <#  Returns total lag for the processor group, or $null if Kafka cannot be
        reached. Never returns 0 for "unknown" — a load test that silently
        reports success when the broker is down is worse than no test.       #>
    $out = docker compose exec -T kafka kafka-consumer-groups `
        --bootstrap-server localhost:9092 --describe --group log-processor 2>$null
    $values = $out | Select-String "raw-logs" | ForEach-Object {
        ($_ -split '\s+')[5]
    } | Where-Object { $_ -match '^\d+$' } | ForEach-Object { [int]$_ }

    if (-not $values) { return $null }
    return ($values | Measure-Object -Sum).Sum
}

function Assert-ProcessorRunning {
    try {
        $r = Invoke-WebRequest "http://localhost:8000/readyz" -UseBasicParsing -TimeoutSec 10
        $state = $r.Content | ConvertFrom-Json
        if (-not $state.ready) { throw "processor is not ready" }
        Write-Host ("processor ready, {0} partitions assigned" -f $state.assigned_partitions)
    } catch {
        throw "Processor is not reachable on :8000. Start it first with 'make process'."
    }
}

Write-Host "=== M6 load test ==="
Assert-ProcessorRunning

$startLag = Get-Lag
if ($null -eq $startLag) { throw "Cannot read consumer lag - is Kafka up?" }
Write-Host ("starting lag: {0:N0}" -f $startLag)
Write-Host ""

# --- Phase 1: baseline -------------------------------------------------------
Write-Host ("Phase 1  baseline {0}/s for {1}s" -f $BaselineRate, $BaselineSeconds)
$baseline = Start-Process -PassThru -NoNewWindow -FilePath "python" -ArgumentList @(
    "-m", "producer.scenarios", "--scenario", "burst",
    "--rate", $BaselineRate, "--duration", $BaselineSeconds
)
$baseline.WaitForExit()
$lagAfterBaseline = Get-Lag
Write-Host ("  lag after baseline: {0:N0}" -f $lagAfterBaseline)
Write-Host ""

# --- Phase 2: the spike ------------------------------------------------------
$spikeRate = $BaselineRate * $SpikeMultiplier
Write-Host ("Phase 2  spike {0}/s ({1}x) for {2}s" -f $spikeRate, $SpikeMultiplier, $SpikeSeconds)
$spikeStart = Get-Date
$spike = Start-Process -PassThru -NoNewWindow -FilePath "python" -ArgumentList @(
    "-m", "producer.scenarios", "--scenario", "burst",
    "--rate", $spikeRate, "--duration", $SpikeSeconds
)

$peakLag = 0
while (-not $spike.HasExited) {
    Start-Sleep -Seconds 5
    $lag = Get-Lag
    if ($null -ne $lag) {
        if ($lag -gt $peakLag) { $peakLag = $lag }
        Write-Host ("  t+{0,3}s  lag = {1,8:N0}" -f [int]((Get-Date) - $spikeStart).TotalSeconds, $lag)
    }
}
Write-Host ("  peak lag during spike: {0:N0}" -f $peakLag)
Write-Host ""

# --- Phase 3: drain ----------------------------------------------------------
Write-Host "Phase 3  measuring drain to zero"
$drainStart = Get-Date
$drained = $false
while (((Get-Date) - $drainStart).TotalSeconds -lt $MaxDrainSeconds) {
    $lag = Get-Lag
    $elapsed = [int]((Get-Date) - $drainStart).TotalSeconds
    if ($null -eq $lag) { Write-Host "  lag unavailable"; Start-Sleep -Seconds 3; continue }
    Write-Host ("  t+{0,3}s  lag = {1,8:N0}" -f $elapsed, $lag)
    if ($lag -le 0) { $drained = $true; break }
    Start-Sleep -Seconds 3
}
$drainSeconds = [int]((Get-Date) - $drainStart).TotalSeconds

# --- Verdict -----------------------------------------------------------------
Write-Host ""
Write-Host "=== Result ==="
Write-Host ("baseline rate      {0:N0}/s" -f $BaselineRate)
Write-Host ("spike rate         {0:N0}/s ({1}x for {2}s)" -f $spikeRate, $SpikeMultiplier, $SpikeSeconds)
Write-Host ("peak lag           {0:N0}" -f $peakLag)
if ($drained) {
    $verdict = if ($drainSeconds -lt 60) { "PASS" } else { "FAIL" }
    Write-Host ("drain to zero      {0}s   (PRD target: <60s)  {1}" -f $drainSeconds, $verdict)
} else {
    Write-Host ("drain to zero      DID NOT DRAIN within {0}s" -f $MaxDrainSeconds)
}
