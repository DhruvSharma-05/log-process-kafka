<#
    End-to-end demo (M7).

        powershell -ExecutionPolicy Bypass -File scripts\demo.ps1
        powershell -ExecutionPolicy Bypass -File scripts\demo.ps1 -Fast -NoPause

    Walks the whole pipeline in about six minutes: healthy traffic, an injected
    error spike that trips the FR4.2 alert, corrupt lines that land in the
    dead-letter queue, and the pipeline's own observability. Pauses between acts
    so there is time to watch the dashboards.

    Prerequisites:
        make up && make topics && make ch-init && make ksql-init
        make process          # in another terminal, left running

    Note: SQL is built as single-line strings on purpose. PowerShell 5.1
    here-strings are whitespace-sensitive in ways that break silently.
#>
[CmdletBinding()]
param(
    [switch]$Fast,          # shorter phases, for a rehearsal
    [switch]$NoPause        # never wait for a keypress
)

$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)

$baselineSeconds = if ($Fast) { 45 } else { 120 }
$spikeSeconds    = if ($Fast) { 90 } else { 180 }
$corruptSeconds  = if ($Fast) { 30 } else { 60 }

function Write-Act {
    param([string]$Number, [string]$Title, [string]$Watch)
    Write-Host ""
    Write-Host ("=" * 72)
    Write-Host "  ACT $Number  $Title"
    Write-Host ("=" * 72)
    Write-Host "  Watch: $Watch"
    Write-Host ""
}

function Wait-ForReader {
    if ($NoPause) { return }
    Write-Host ""
    Read-Host "  Press Enter to continue" | Out-Null
}

function Invoke-ClickHouse {
    param([string]$Sql)
    docker compose exec -T clickhouse clickhouse-client --query $Sql 2>$null
}

function Show-TopicMessages {
    <#  kafka-console-consumer exits non-zero when --timeout-ms elapses, which
        under $ErrorActionPreference='Stop' would abort the whole demo. Reading
        a topic tail is informational, so failure here must never be fatal.  #>
    param(
        [string]$Topic,
        [int]$Max = 3,
        [int]$TimeoutMs = 20000
    )
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $out = docker compose exec -T kafka kafka-console-consumer `
            --bootstrap-server localhost:9092 --topic $Topic `
            --from-beginning --timeout-ms $TimeoutMs 2>$null
        $messages = @($out | Where-Object { $_ -match '^\s*\{' } | Select-Object -Last $Max)
        if ($messages.Count -gt 0) {
            $messages | ForEach-Object { Write-Host "    $_" }
        } else {
            Write-Host "    (no messages in $Topic yet)"
        }
    } catch {
        Write-Host "    (could not read $Topic)"
    } finally {
        $ErrorActionPreference = $previous
        $global:LASTEXITCODE = 0
    }
}

function Test-Prerequisites {
    try {
        $state = (Invoke-WebRequest "http://localhost:8000/readyz" -UseBasicParsing -TimeoutSec 10).Content | ConvertFrom-Json
        if (-not $state.ready) { throw "processor reports not ready" }
        Write-Host ("  processor:  ready, {0} partitions assigned" -f $state.assigned_partitions)
    } catch {
        throw "Processor not running. Start it with 'make process' in another terminal."
    }
    try {
        Invoke-WebRequest "http://localhost:8123/ping" -UseBasicParsing -TimeoutSec 10 | Out-Null
        Write-Host "  clickhouse: up"
    } catch {
        throw "ClickHouse not reachable on :8123."
    }
    try {
        Invoke-WebRequest "http://localhost:3000/api/health" -UseBasicParsing -TimeoutSec 10 | Out-Null
        Write-Host "  grafana:    up"
    } catch {
        Write-Warning "Grafana not reachable on :3000 - dashboards will not be visible."
    }
}

function Show-ServiceTable {
    param([string]$Window = "5 MINUTE")
    $sql = "SELECT service, count() AS requests, countIf(is_error) AS errors, " +
           "round(100*countIf(is_error)/count(), 2) AS err_pct, " +
           "round(quantile(0.95)(response_time_ms)) AS p95_ms " +
           "FROM logpipe.logs_hot WHERE stored_at > now() - INTERVAL $Window " +
           "GROUP BY service ORDER BY err_pct DESC, requests DESC " +
           "FORMAT PrettyCompactMonoBlock"
    Invoke-ClickHouse $sql
}

function Show-AlertState {
    try {
        $auth = @{ Authorization = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:admin")) }
        $r = Invoke-RestMethod "http://localhost:3000/api/prometheus/grafana/api/v1/rules" -Headers $auth -TimeoutSec 20
        foreach ($g in $r.data.groups) {
            foreach ($rule in $g.rules) {
                Write-Host ("    {0,-40} {1}" -f $rule.name, $rule.state.ToUpper())
            }
        }
    } catch {
        Write-Host "    (could not read alert state)"
    }
}

function Show-Lag {
    foreach ($g in @("log-processor", "clickhouse-parsed")) {
        $out = docker compose exec -T kafka kafka-consumer-groups `
            --bootstrap-server localhost:9092 --describe --group $g 2>$null
        $values = $out | Select-String -Pattern "\s(raw-logs|parsed-logs)\s" | ForEach-Object {
            ($_ -split '\s+')[5]
        } | Where-Object { $_ -match '^\d+$' }
        if ($values) {
            Write-Host ("    {0,-22} {1,8:N0}" -f $g, (($values | Measure-Object -Sum).Sum))
        } else {
            Write-Host ("    {0,-22} {1,8}" -f $g, "n/a")
        }
    }
}

Write-Host ""
Write-Host "Real-Time Log Processing Pipeline - end-to-end demo"
Write-Host ""
Test-Prerequisites

# ---------------------------------------------------------------------------
Write-Act "1" "Healthy traffic across seven services" `
    "http://localhost:3000 -> Logpipe -> Service Health"

Write-Host "  Baseline traffic for ${baselineSeconds}s at ~0.4% errors, as production looks."
Write-Host ""
python -m producer.scenarios --scenario baseline --rate 200 --duration $baselineSeconds

Write-Host ""
Write-Host "  Per-service view from ClickHouse:"
Show-ServiceTable
Wait-ForReader

# ---------------------------------------------------------------------------
Write-Act "2" "checkout-api degrades - the FR4.2 alert fires" `
    "the error-rate panel crossing the red 5% threshold line"

Write-Host "  Injecting 35% errors on checkout-api only. Everything else stays healthy,"
Write-Host "  so the alert has to name the right culprit rather than firing globally."
Write-Host ""
python -m producer.scenarios --scenario error-spike --service checkout-api `
    --error-rate 0.35 --rate 150 --duration $spikeSeconds

Write-Host ""
Write-Host "  Per-service view - note checkout-api's error rate and p95 latency:"
Show-ServiceTable
Write-Host ""
Write-Host "  Alert state:"
Show-AlertState
Write-Host ""
Write-Host "  ksqlDB 1-minute windowed aggregates (FR2.4), most recent windows:"
Show-TopicMessages -Topic "service-metrics-1m" -Max 4
Wait-ForReader

# ---------------------------------------------------------------------------
Write-Act "3" "Malformed logs go to the DLQ, not into the pipeline" `
    "http://localhost:8080 -> Topics -> dead-letter-queue -> Messages"

Write-Host "  Injecting 5% unparseable lines. The processor must not restart, and"
Write-Host "  valid traffic must keep flowing."
Write-Host ""
python -m producer.scenarios --scenario malformed --corrupt-rate 0.05 `
    --rate 200 --duration $corruptSeconds

Write-Host ""
Write-Host "  Dead-lettered events, with the stage and reason that rejected them:"
Show-TopicMessages -Topic "dead-letter-queue" -Max 3
Wait-ForReader

# ---------------------------------------------------------------------------
Write-Act "4" "The pipeline observing itself" `
    "http://localhost:3000 -> Logpipe -> Pipeline Health"

Write-Host "  Consumer lag per group (FR5.1):"
Show-Lag

Write-Host ""
Write-Host "  End-to-end ingestion latency (FR3.1, target < 10s p95):"
$latencySql = "SELECT count() AS events, " +
              "round(quantile(0.50)(dateDiff('millisecond', ingested_at, stored_at))/1000, 2) AS p50_s, " +
              "round(quantile(0.95)(dateDiff('millisecond', ingested_at, stored_at))/1000, 2) AS p95_s, " +
              "round(quantile(0.99)(dateDiff('millisecond', ingested_at, stored_at))/1000, 2) AS p99_s " +
              "FROM logpipe.logs_hot WHERE stored_at > now() - INTERVAL 10 MINUTE " +
              "FORMAT PrettyCompactMonoBlock"
Invoke-ClickHouse $latencySql

Write-Host ""
Write-Host "  Storage tiers (hot local disk vs S3/MinIO):"
$partsSql = "SELECT partition, disk_name, sum(rows) AS rows, " +
            "formatReadableSize(sum(bytes_on_disk)) AS size " +
            "FROM system.parts WHERE database='logpipe' AND table='logs_hot' AND active " +
            "GROUP BY partition, disk_name ORDER BY partition " +
            "FORMAT PrettyCompactMonoBlock"
Invoke-ClickHouse $partsSql

Write-Host ""
Write-Host ("=" * 72)
Write-Host "  Demo complete."
Write-Host ""
Write-Host "  The alert resolves on its own once the 5-minute window clears."
Write-Host "  For the hardening evidence (broker failure, SASL, 2x burst drain):"
Write-Host "      make loadtest      # 2x spike, measure drain"
Write-Host "      make prod-up       # 3 brokers, RF=3, SASL"
Write-Host "      make prod-auth     # authentication is enforced"
Write-Host ("=" * 72)
Write-Host ""
