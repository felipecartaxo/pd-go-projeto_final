[CmdletBinding()]
param(
    [switch]$SkipRace,
    [switch]$Stress,
    [switch]$RunLoad
)

# Executes the reproducible local acceptance workflow described in
# docs/GUIA_EXECUCAO_TESTES.md. It only stops processes started by this script.
$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$runDir = Join-Path $root "test-results\\$stamp"
New-Item -ItemType Directory -Path $runDir -Force | Out-Null
$commandLog = Join-Path $runDir 'commands.txt'

$broker1 = $null
$broker2 = $null
$subscriberB2 = $null
$subscriberFailover = $null

function Test-UdpPortAvailable {
    param([int]$Port)

    $probe = $null
    try {
        $probe = [System.Net.Sockets.UdpClient]::new()
        $endpoint = [System.Net.IPEndPoint]::new([System.Net.IPAddress]::Loopback, $Port)
        $probe.Client.Bind($endpoint)
        return $true
    }
    catch [System.Net.Sockets.SocketException] {
        return $false
    }
    finally {
        if ($null -ne $probe) {
            $probe.Dispose()
        }
    }
}

function Write-CommandLog {
    param([string]$Text)
    Add-Content -LiteralPath $commandLog -Value "> $Text"
}

function Invoke-Checked {
    param(
        [string]$Name,
        [string]$Program,
        [string[]]$Arguments
    )

    Write-Host "== $Name ==" -ForegroundColor Cyan
    Write-CommandLog "$Program $($Arguments -join ' ')"
    $log = Join-Path $runDir "$Name.log"
    & $Program @Arguments *>&1 | Tee-Object -LiteralPath $log
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed with exit code $LASTEXITCODE. See $log"
    }
}

function Invoke-ExpectedFailure {
    param(
        [string]$Name,
        [string]$Program,
        [string[]]$Arguments,
        [string]$ExpectedText
    )

    Write-Host "== $Name (expected failure) ==" -ForegroundColor Cyan
    Write-CommandLog "$Program $($Arguments -join ' ')"
    $log = Join-Path $runDir "$Name.log"
    $previousPreference = $ErrorActionPreference
    try {
        # ERROR replies are expected in this branch and arrive on stderr from
        # log.Fatal in cmd/demo; preserve them as output instead of aborting.
        $ErrorActionPreference = 'Continue'
        $output = & $Program @Arguments *>&1 | Tee-Object -LiteralPath $log
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($exitCode -eq 0) {
        throw "$Name unexpectedly succeeded. See $log"
    }
    if (($output | Out-String) -notmatch $ExpectedText) {
        throw "$Name failed, but did not contain '$ExpectedText'. See $log"
    }
}

function Invoke-Experimental {
    param(
        [string]$Name,
        [string]$Program,
        [string[]]$Arguments
    )

    Write-Host "== $Name (experimental) ==" -ForegroundColor Cyan
    Write-CommandLog "$Program $($Arguments -join ' ')"
    $log = Join-Path $runDir "$Name.log"
    $previousPreference = $ErrorActionPreference
    try {
        # A fatal Go runtime error writes to stderr. It is evidence of the
        # environment limitation, but must not prevent functional validation.
        $ErrorActionPreference = 'Continue'
        & $Program @Arguments *>&1 | Tee-Object -LiteralPath $log
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousPreference
    }
    if ($exitCode -ne 0) {
        Write-Warning "$Name stopped with exit code $exitCode. Its log was retained in $log; see docs/GUIA_EXECUCAO_TESTES.md."
        return $false
    }
    return $true
}

function Start-TestProcess {
    param(
        [string]$Name,
        [string]$Program,
        [string[]]$Arguments
    )

    Write-CommandLog "$Program $($Arguments -join ' ')"
    $stdout = Join-Path $runDir "$Name.stdout.log"
    $stderr = Join-Path $runDir "$Name.stderr.log"
    return Start-Process -FilePath $Program -ArgumentList $Arguments -PassThru -NoNewWindow `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr
}

function Wait-ForText {
    param(
        [string]$Path,
        [string]$Text,
        [int]$TimeoutSeconds = 8
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if ((Test-Path -LiteralPath $Path) -and ((Get-Content -LiteralPath $Path -Raw) -match $Text)) {
            return
        }
        Start-Sleep -Milliseconds 200
    }
    throw "Timed out waiting for '$Text' in $Path"
}

function Stop-TestProcess {
    param($Process)
    if ($null -ne $Process -and -not $Process.HasExited) {
        Stop-Process -Id $Process.Id -ErrorAction SilentlyContinue
        $Process.WaitForExit(5000) | Out-Null
    }
}

try {
	$busyPorts = @(9001, 9002 | Where-Object { -not (Test-UdpPortAvailable $_) })
	if ($busyPorts.Count -gt 0) {
		throw "UDP port(s) $($busyPorts -join ', ') are already in use. Stop manually started brokers with Ctrl+C before running this script."
	}

    Invoke-Checked 'go-test' 'go' @('test', './...')
    Invoke-Checked 'integration' 'go' @('test', './integration', '-v')
    Invoke-Checked 'no-subscribers' 'go' @('test', './internal/broker', '-run', 'TestNoSubscribersRetriesUntilACKOrLimit', '-v')
    Invoke-Checked 'rate-limit' 'go' @('test', './internal/broker', '-run', 'TestRateLimitIsPerPublisher', '-v')
    Invoke-Checked 'drop-oldest-fairness' 'go' @('test', './internal/broker', '-run', 'TestInputDropOldestAndFairDispatch', '-v')
    Invoke-Checked 'slow-subscriber' 'go' @('test', './internal/broker', '-run', 'TestSlowSubscriberOutputQueueDropsOldestWithoutBlockingOthers', '-v')

    $brokerExe = Join-Path $runDir 'broker.exe'
    $demoExe = Join-Path $runDir 'demo.exe'
    $loadgenExe = Join-Path $runDir 'loadgen.exe'
    Invoke-Checked 'build-broker' 'go' @('build', '-o', $brokerExe, './cmd/broker')
    Invoke-Checked 'build-demo' 'go' @('build', '-o', $demoExe, './cmd/demo')
    if ($RunLoad) {
        Invoke-Checked 'build-loadgen' 'go' @('build', '-o', $loadgenExe, './cmd/loadgen')
    }

    $broker1 = Start-TestProcess 'broker-1' $brokerExe @('-id', '1')
    $broker2 = Start-TestProcess 'broker-2' $brokerExe @('-id', '2')
    # log/slog writes its operational records to stderr by default.
    Wait-ForText (Join-Path $runDir 'broker-1.stderr.log') 'broker listening'
    Wait-ForText (Join-Path $runDir 'broker-2.stderr.log') 'broker listening'

    $brokers = '127.0.0.1:9001,127.0.0.1:9002'
    $topic = "teste/manual-$stamp"
    Invoke-Checked 'create-topic-b1' $demoExe @('-mode', 'create', '-client-id', 'admin-1', '-prefer', '1', '-brokers', $brokers, '-topic', $topic)
    Start-Sleep -Seconds 1 # catalog replication from B1 to B2 is asynchronous

    $subscriberB2 = Start-TestProcess 'subscriber-b2' $demoExe @('-mode', 'sub', '-client-id', 'sub-2', '-prefer', '2', '-brokers', $brokers, '-topic', $topic)
    Start-Sleep -Seconds 2 # permits interest propagation from B2 to B1
    Invoke-Checked 'publish-b1' $demoExe @('-mode', 'pub', '-client-id', 'pub-1', '-prefer', '1', '-brokers', $brokers, '-topic', $topic, '-payload', '{\"valor\":120,\"lote\":3}')
    Wait-ForText (Join-Path $runDir 'subscriber-b2.stdout.log') $topic
    Wait-ForText (Join-Path $runDir 'subscriber-b2.stdout.log') '"valor":120'

    # B2 must forward topic mutations to B1 while B1 is alive.
    $topicFromB2 = "$topic-b2"
    Invoke-Checked 'create-topic-via-b2' $demoExe @('-mode', 'create', '-client-id', 'admin-2', '-prefer', '2', '-brokers', $brokers, '-topic', $topicFromB2)

    if ($RunLoad) {
		$fanoutSubscribers = if ($Stress) { 100 } else { 20 }
		$fairnessSubscribers = if ($Stress) { 30 } else { 20 }
        # Each scenario owns a fresh topic. Besides isolating measurements, it
        # avoids the expected TOPIC_ALREADY_EXISTS notice on stderr from a
        # previous loadgen run.
        # Go 1.26.1 on Windows faulted inside its UDP runtime with parallel
        # reader threads. One logical processor retains concurrent clients
        # while producing a reproducible local workload.
        $loadRuntimeArgs = @('-gomaxprocs', '1')
        $burstArgs = @('-scenario', 'burst', '-duration', '15s', '-publishers', '5', '-subscribers', '20', '-brokers', $brokers, '-topic', "loadgen/burst-$stamp") + $loadRuntimeArgs + @('-output', (Join-Path $runDir 'results-burst.csv'))
        $fanoutArgs = @('-scenario', 'fanout', '-duration', '30s', '-publishers', '1', '-subscribers', "$fanoutSubscribers", '-brokers', $brokers, '-topic', "loadgen/fanout-$stamp") + $loadRuntimeArgs + @('-output', (Join-Path $runDir 'results-fanout.csv'))
        $fairnessArgs = @('-scenario', 'fairness', '-duration', '30s', '-publishers', '2', '-subscribers', "$fairnessSubscribers", '-brokers', $brokers, '-topic', "loadgen/fairness-$stamp") + $loadRuntimeArgs + @('-output', (Join-Path $runDir 'results-fairness.csv'))

        $null = Invoke-Experimental 'load-burst' $loadgenExe $burstArgs
        $null = Invoke-Experimental 'load-fanout' $loadgenExe $fanoutArgs
        $null = Invoke-Experimental 'load-fairness' $loadgenExe $fairnessArgs
    }
    else {
        Add-Content -LiteralPath $commandLog -Value '# loadgen skipped; rerun with -RunLoad to execute experimental workloads'
        Write-Host '== loadgen skipped (use -RunLoad to execute experimental workloads) ==' -ForegroundColor Yellow
    }

    $subscriberFailover = Start-TestProcess 'subscriber-failover' $demoExe @('-mode', 'sub', '-client-id', 'sub-failover', '-prefer', '1', '-brokers', $brokers, '-topic', $topic)
    # demo/sub has no "ready" banner; allow REGISTER and SUBSCRIBE to finish
    # before inducing the B1 failure.
    Start-Sleep -Seconds 3
    Stop-TestProcess $broker1
    $broker1 = $null
    Start-Sleep -Seconds 5 # detection, REGISTER on B2 and desired re-subscribe
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        Invoke-Checked "publish-via-b2-after-b1-failure-$attempt" $demoExe @('-mode', 'pub', '-client-id', "pub-2-$attempt", '-prefer', '2', '-brokers', $brokers, '-topic', $topic, '-payload', '{\"valor\":130,\"lote\":3}')
        if ((Test-Path (Join-Path $runDir 'subscriber-failover.stdout.log')) -and ((Get-Content (Join-Path $runDir 'subscriber-failover.stdout.log') -Raw) -match '"valor":130')) {
            break
        }
        Start-Sleep -Seconds 2
    }
    Wait-ForText (Join-Path $runDir 'subscriber-failover.stdout.log') '"valor":130' 10
    Invoke-ExpectedFailure 'create-with-b1-down' $demoExe @('-mode', 'create', '-client-id', 'admin-b2', '-prefer', '2', '-brokers', $brokers, '-topic', "$topic-unavailable") 'COORDINATOR_UNAVAILABLE|INCONCLUSIVE'

    $broker1 = Start-TestProcess 'broker-1-restarted' $brokerExe @('-id', '1')
    Wait-ForText (Join-Path $runDir 'broker-1-restarted.stderr.log') 'broker listening'
    Start-Sleep -Seconds 2 # allows PEER_SYNC after B1 restart

    if (-not $SkipRace) {
        Write-Host '== race detector (may require CGO on Windows) ==' -ForegroundColor Cyan
        Write-CommandLog 'go test -race ./...'
        & go test -race ./... *>&1 | Tee-Object -LiteralPath (Join-Path $runDir 'race.log')
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Race detector did not run successfully; its output was retained in $runDir\\race.log."
        }
    }

    Write-Host "All required checks completed. Evidence: $runDir" -ForegroundColor Green
}
finally {
    Stop-TestProcess $subscriberFailover
    Stop-TestProcess $subscriberB2
    Stop-TestProcess $broker1
    Stop-TestProcess $broker2
}
