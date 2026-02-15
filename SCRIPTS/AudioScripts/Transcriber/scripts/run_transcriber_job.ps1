param(
    [Parameter(Mandatory = $true)]
    [string]$MainFolderPath,

    [Parameter(Mandatory = $true)]
    [string]$ChannelName,

    [Parameter(Mandatory = $true)]
    [string]$VideoFolderName,

    [string]$Device = "cpu",
    [string]$Language = "",
    [string]$Mode = "",
    [string]$TextPartsPath = "",
    [string]$TextFileName = "",
    [int]$ChunkLimit = 0,
    [int]$MinIndexChars = 35,
    [int]$MinIndexTokens = 0,
    [int]$AsrWorkers = 1,
    [double]$MinFreeVramGb = 0.0,
    [string]$AsrPrompt = "",
    [int]$AsrPromptMaxChars = 800,
    [string]$JobId = "",
    [string]$ProjectDir = "",
    [string]$JobsRoot = "",
    [string]$PythonExe = "",
    [string]$OutFilePath = "",
    [string[]]$ExtraArgs = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Test-PythonCandidate {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Exe,
        [string[]]$PrefixArgs = @()
    )
    try {
        & $Exe @PrefixArgs -c "import whisperx, rapidfuzz, unidecode"
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    }
}

function Write-ProgressSnapshot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ProgressPath
    )

    if (-not (Test-Path -LiteralPath $ProgressPath)) {
        return
    }
    try {
        $raw = Get-Content -LiteralPath $ProgressPath -Raw -Encoding UTF8
        if ([string]::IsNullOrWhiteSpace($raw)) { return }
        $obj = $raw | ConvertFrom-Json
        $p = $obj.meta.progress
        if ($null -eq $p) { return }
        $status = [string]($p.status)
        $current = [int]($p.current)
        $total = [int]($p.total)
        $audDone = [int]($p.audios_done)
        $audTotal = [int]($p.audios_total)
        $lastAudio = [string]($p.last_audio)
        $timeSpent = [double]($p.time_spent)
        Write-Host "[progress] status=$status sentences=$current/$total audios=$audDone/$audTotal last='$lastAudio' time_spent=$timeSpent"
    } catch {
        Write-Host "[progress] unable_to_parse: $($_.Exception.Message)"
    }
}

if ([string]::IsNullOrWhiteSpace($ProjectDir)) {
    $ProjectDir = Split-Path -Parent $PSScriptRoot
}
$ProjectDir = [System.IO.Path]::GetFullPath($ProjectDir)

if ([string]::IsNullOrWhiteSpace($JobsRoot)) {
    $JobsRoot = Join-Path $ProjectDir "remote_jobs"
}
$JobsRoot = [System.IO.Path]::GetFullPath($JobsRoot)
New-Item -ItemType Directory -Path $JobsRoot -Force | Out-Null

if ([string]::IsNullOrWhiteSpace($JobId)) {
    $JobId = [DateTime]::UtcNow.ToString("yyyyMMdd-HHmmss") + "-" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
}

$jobDir = Join-Path $JobsRoot $JobId
New-Item -ItemType Directory -Path $jobDir -Force | Out-Null

if ([string]::IsNullOrWhiteSpace($OutFilePath)) {
    $OutFilePath = Join-Path $jobDir "transcript.json"
}
$OutFilePath = [System.IO.Path]::GetFullPath($OutFilePath)
$progressPath = Join-Path (Split-Path -Parent $OutFilePath) (([System.IO.Path]::GetFileNameWithoutExtension($OutFilePath)) + "Progress" + ([System.IO.Path]::GetExtension($OutFilePath)))
$runLog = Join-Path $jobDir "run.log"
$statusPath = Join-Path $jobDir "status.json"
$paramsPath = Join-Path $jobDir "params.json"

$paramsObj = [ordered]@{
    job_id = $JobId
    mainFolderPath = $MainFolderPath
    channelName = $ChannelName
    videoFolderName = $VideoFolderName
    device = $Device
    language = $Language
    mode = $Mode
    textPartsPath = $TextPartsPath
    textFileName = $TextFileName
    chunkLimit = $ChunkLimit
    minIndexChars = $MinIndexChars
    minIndexTokens = $MinIndexTokens
    asrWorkers = $AsrWorkers
    minFreeVramGb = $MinFreeVramGb
    asrPrompt = $AsrPrompt
    asrPromptMaxChars = $AsrPromptMaxChars
    outFilePath = $OutFilePath
    extraArgs = $ExtraArgs
    projectDir = $ProjectDir
    jobsRoot = $JobsRoot
    createdUtc = (Get-Date).ToUniversalTime().ToString("o")
}
$paramsObj | ConvertTo-Json -Depth 64 | Set-Content -LiteralPath $paramsPath -Encoding UTF8

$scriptPath = Join-Path $ProjectDir "transcribe.py"
if (-not (Test-Path -LiteralPath $scriptPath)) {
    throw "transcribe.py not found at $scriptPath"
}

$candidateList = New-Object System.Collections.Generic.List[object]

if (-not [string]::IsNullOrWhiteSpace($PythonExe)) {
    $candidateList.Add(@{ exe = $PythonExe; prefix = @() }) | Out-Null
}

$projectVenvPython = Join-Path $ProjectDir ".venv\Scripts\python.exe"
if (Test-Path -LiteralPath $projectVenvPython) {
    $candidateList.Add(@{ exe = $projectVenvPython; prefix = @() }) | Out-Null
}

$minicondaPython = Join-Path $env:USERPROFILE "miniconda3\python.exe"
if (Test-Path -LiteralPath $minicondaPython) {
    $candidateList.Add(@{ exe = $minicondaPython; prefix = @() }) | Out-Null
}

$pythonCmd = Get-Command "python" -ErrorAction SilentlyContinue
if ($pythonCmd) {
    $candidateList.Add(@{ exe = $pythonCmd.Source; prefix = @() }) | Out-Null
}

$pyCmd = Get-Command "py" -ErrorAction SilentlyContinue
if ($pyCmd) {
    $candidateList.Add(@{ exe = $pyCmd.Source; prefix = @("-3") }) | Out-Null
}

if ($candidateList.Count -eq 0) {
    throw "Python executable not found (checked PythonExe, miniconda3, python, py -3)."
}

$pythonCommand = ""
$pythonPrefixArgs = @()
foreach ($cand in $candidateList) {
    $exe = [string]$cand.exe
    $prefix = @($cand.prefix)
    if (-not (Test-Path -LiteralPath $exe) -and -not (Get-Command $exe -ErrorAction SilentlyContinue)) {
        continue
    }
    if (Test-PythonCandidate -Exe $exe -PrefixArgs $prefix) {
        $pythonCommand = $exe
        $pythonPrefixArgs = $prefix
        break
    }
}

if ([string]::IsNullOrWhiteSpace($pythonCommand)) {
    $first = $candidateList[0]
    throw "No Python interpreter with required modules (whisperx, rapidfuzz, unidecode). First candidate was '$($first.exe)'."
}

Write-Host "[python] using=$pythonCommand prefix=$($pythonPrefixArgs -join ' ')"

$argList = @(
    $scriptPath,
    "--mainFolderPath", $MainFolderPath,
    "--channelName", $ChannelName,
    "--videoFolderName", $VideoFolderName,
    "--device", $Device,
    "--outFilePath", $OutFilePath
)

if (-not [string]::IsNullOrWhiteSpace($Language)) {
    $argList += @("--language", $Language)
}
if (-not [string]::IsNullOrWhiteSpace($Mode)) {
    $argList += @("--mode", $Mode)
}
if (-not [string]::IsNullOrWhiteSpace($TextPartsPath)) {
    $argList += @("--textPartsPath", $TextPartsPath)
}
if (-not [string]::IsNullOrWhiteSpace($TextFileName)) {
    $argList += @("--textFileName", $TextFileName)
}
if ($ChunkLimit -gt 0) {
    $argList += @("--chunkLimit", [string]$ChunkLimit)
}
if ($MinIndexChars -ge 0) {
    $argList += @("--minIndexChars", [string]$MinIndexChars)
}
if ($MinIndexTokens -ge 0) {
    $argList += @("--minIndexTokens", [string]$MinIndexTokens)
}
if ($AsrWorkers -ge 1) {
    $argList += @("--asrWorkers", [string]$AsrWorkers)
}
if ($MinFreeVramGb -ge 0) {
    $argList += @("--minFreeVramGb", [string]$MinFreeVramGb)
}
if (-not [string]::IsNullOrWhiteSpace($AsrPrompt)) {
    $argList += @("--asrPrompt", $AsrPrompt)
}
if ($AsrPromptMaxChars -gt 0) {
    $argList += @("--asrPromptMaxChars", [string]$AsrPromptMaxChars)
}
if ($ExtraArgs -and $ExtraArgs.Count -gt 0) {
    $argList += $ExtraArgs
}

$execArgs = @($pythonPrefixArgs) + @($argList)

$cmdPreview = @($pythonCommand) + $execArgs
Write-Host "[job] id=$JobId"
Write-Host "[job] dir=$jobDir"
Write-Host "[job] transcript=$OutFilePath"
Write-Host "[job] progress=$progressPath"
Write-Host "[job] cmd=$($cmdPreview -join ' ')"
Write-Host "[job] params=$paramsPath"
Write-Host "[job] status=$statusPath"

$stdOutPath = Join-Path $jobDir "stdout.log"
$stdErrPath = Join-Path $jobDir "stderr.log"
Set-Content -LiteralPath $stdOutPath -Value "" -Encoding UTF8
Set-Content -LiteralPath $stdErrPath -Value "" -Encoding UTF8

$proc = Start-Process `
    -FilePath $pythonCommand `
    -ArgumentList $execArgs `
    -WorkingDirectory $ProjectDir `
    -NoNewWindow `
    -Wait `
    -PassThru `
    -RedirectStandardOutput $stdOutPath `
    -RedirectStandardError $stdErrPath

$exitCode = [int]$proc.ExitCode

$stdoutText = ""
$stderrText = ""
if (Test-Path -LiteralPath $stdOutPath) {
    $stdoutText = Get-Content -LiteralPath $stdOutPath -Raw -Encoding UTF8
}
if (Test-Path -LiteralPath $stdErrPath) {
    $stderrText = Get-Content -LiteralPath $stdErrPath -Raw -Encoding UTF8
}
Set-Content -LiteralPath $runLog -Value ($stdoutText + $stderrText) -Encoding UTF8

if (-not [string]::IsNullOrEmpty($stdoutText)) {
    Write-Host $stdoutText
}
if (-not [string]::IsNullOrEmpty($stderrText)) {
    Write-Host $stderrText
}

if (Test-Path -LiteralPath $progressPath) {
    Write-ProgressSnapshot -ProgressPath $progressPath
}

$progressStatus = ""
if (Test-Path -LiteralPath $progressPath) {
    try {
        $pRaw = Get-Content -LiteralPath $progressPath -Raw -Encoding UTF8
        if (-not [string]::IsNullOrWhiteSpace($pRaw)) {
            $pObj = $pRaw | ConvertFrom-Json
            if ($pObj -and $pObj.meta -and $pObj.meta.progress -and $pObj.meta.progress.status) {
                $progressStatus = [string]$pObj.meta.progress.status
            }
        }
    } catch {
        $progressStatus = ""
    }
}

$ok = $false
if (-not [string]::IsNullOrWhiteSpace($progressStatus)) {
    $ok = $progressStatus -in @("success", "done")
} elseif ($exitCode -eq 0 -and (Test-Path -LiteralPath $OutFilePath)) {
    $ok = $true
}

$statusObj = [ordered]@{
    job_id = $JobId
    ok = $ok
    exit_code = $exitCode
    progress_status = $progressStatus
    transcript_path = $OutFilePath
    progress_path = $progressPath
    run_log = $runLog
    stdout_log = $stdOutPath
    stderr_log = $stdErrPath
    finishedUtc = (Get-Date).ToUniversalTime().ToString("o")
}
$statusObj | ConvertTo-Json -Depth 64 | Set-Content -LiteralPath $statusPath -Encoding UTF8

if (Test-Path -LiteralPath $progressPath) {
    Write-ProgressSnapshot -ProgressPath $progressPath
}

if ($ok) {
    Write-Host "[job] success"
    Write-Host "[result] transcript=$OutFilePath"
    Write-Host "[result] progress=$progressPath"
    Write-Host "[result] status=$statusPath"
    exit 0
}

Write-Host "[job] failed exit_code=$exitCode"
Write-Host "[result] status=$statusPath"
exit 1
