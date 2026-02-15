param(
    [ValidateSet("cpu", "cuda")]
    [string]$Device = "cpu",
    [string]$ProjectDir = "",
    [string]$VenvDir = ".venv",
    [string]$PythonExe = "",
    [string]$CudaIndexUrl = "https://download.pytorch.org/whl/cu121",
    [switch]$SkipWinget
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[setup] $Message"
}

function Refresh-Path {
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if (-not [string]::IsNullOrWhiteSpace($machinePath) -or -not [string]::IsNullOrWhiteSpace($userPath)) {
        $env:Path = "$machinePath;$userPath"
    }
}

function Test-CommandExists {
    param([Parameter(Mandatory = $true)][string]$Name)
    return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-BootstrapPython {
    param([string]$ExplicitPython)

    $candidates = New-Object System.Collections.Generic.List[object]

    if (-not [string]::IsNullOrWhiteSpace($ExplicitPython)) {
        $candidates.Add(@{ exe = $ExplicitPython; prefix = @() }) | Out-Null
    }

    $pythonCmd = Get-Command "python" -ErrorAction SilentlyContinue
    if ($pythonCmd) {
        $candidates.Add(@{ exe = $pythonCmd.Source; prefix = @() }) | Out-Null
    }

    $pyCmd = Get-Command "py" -ErrorAction SilentlyContinue
    if ($pyCmd) {
        $candidates.Add(@{ exe = $pyCmd.Source; prefix = @("-3.11") }) | Out-Null
        $candidates.Add(@{ exe = $pyCmd.Source; prefix = @("-3") }) | Out-Null
    }

    foreach ($cand in $candidates) {
        $exe = [string]$cand.exe
        $prefix = @($cand.prefix)
        try {
            & $exe @prefix -c "import sys; assert sys.version_info >= (3,10)"
            if ($LASTEXITCODE -eq 0) {
                return $cand
            }
        } catch {
            continue
        }
    }

    return $null
}

function Ensure-Winget {
    if (Test-CommandExists -Name "winget") {
        return
    }
    throw "winget is not available. Install App Installer from Microsoft Store, then rerun."
}

function Ensure-Python {
    param([string]$ExplicitPython)

    $found = Get-BootstrapPython -ExplicitPython $ExplicitPython
    if ($found -ne $null) {
        return $found
    }

    if ($SkipWinget) {
        throw "Python 3.10+ not found and -SkipWinget specified."
    }

    Ensure-Winget
    Write-Step "Installing Python 3.11 via winget..."
    winget install -e --id Python.Python.3.11 --accept-package-agreements --accept-source-agreements | Out-Host
    Refresh-Path

    $found = Get-BootstrapPython -ExplicitPython $ExplicitPython
    if ($found -eq $null) {
        throw "Python install finished but Python 3.10+ is still not found in PATH."
    }
    return $found
}

function Ensure-FFmpeg {
    $hasFfmpeg = Test-CommandExists -Name "ffmpeg"
    $hasFfprobe = Test-CommandExists -Name "ffprobe"
    if ($hasFfmpeg -and $hasFfprobe) {
        Write-Step "ffmpeg already installed."
        return
    }

    if ($SkipWinget) {
        throw "ffmpeg/ffprobe not found and -SkipWinget specified."
    }

    Ensure-Winget
    Write-Step "Installing ffmpeg via winget..."
    winget install -e --id Gyan.FFmpeg --accept-package-agreements --accept-source-agreements | Out-Host
    Refresh-Path

    $knownBins = @(
        "C:\ffmpeg\bin",
        "$env:ProgramFiles\ffmpeg\bin",
        "$env:ProgramFiles\FFmpeg\bin",
        "$env:LOCALAPPDATA\Microsoft\WinGet\Links"
    )
    foreach ($bin in $knownBins) {
        if (Test-Path -LiteralPath $bin) {
            if (-not ($env:Path -split ";" | Where-Object { $_ -eq $bin })) {
                $env:Path = "$bin;$env:Path"
            }
        }
    }

    $hasFfmpeg = Test-CommandExists -Name "ffmpeg"
    $hasFfprobe = Test-CommandExists -Name "ffprobe"
    if (-not ($hasFfmpeg -and $hasFfprobe)) {
        throw "ffmpeg install did not expose ffmpeg/ffprobe to this shell. Open a new shell and rerun."
    }
}

if ([string]::IsNullOrWhiteSpace($ProjectDir)) {
    $ProjectDir = Split-Path -Parent $PSScriptRoot
}
$ProjectDir = [System.IO.Path]::GetFullPath($ProjectDir)

$requirementsPath = Join-Path $ProjectDir "requirements.txt"
if (-not (Test-Path -LiteralPath $requirementsPath)) {
    throw "requirements.txt not found at $requirementsPath"
}

if ([System.IO.Path]::IsPathRooted($VenvDir)) {
    $venvPath = $VenvDir
} else {
    $venvPath = Join-Path $ProjectDir $VenvDir
}
$venvPath = [System.IO.Path]::GetFullPath($venvPath)
$venvPython = Join-Path $venvPath "Scripts\python.exe"

Write-Step "ProjectDir=$ProjectDir"
Write-Step "VenvDir=$venvPath"
Write-Step "Device=$Device"
if ($Device -eq "cuda") {
    Write-Step "CudaIndexUrl=$CudaIndexUrl"
}

$bootstrapPython = Ensure-Python -ExplicitPython $PythonExe
Write-Step ("Bootstrap Python={0} {1}" -f $bootstrapPython.exe, ($bootstrapPython.prefix -join " "))

Ensure-FFmpeg

if (-not (Test-Path -LiteralPath $venvPython)) {
    Write-Step "Creating virtual environment..."
    & $bootstrapPython.exe @($bootstrapPython.prefix) -m venv $venvPath
} else {
    Write-Step "Virtual environment already exists."
}

if (-not (Test-Path -LiteralPath $venvPython)) {
    throw "venv python not found at $venvPython"
}

Write-Step "Upgrading pip/setuptools/wheel..."
& $venvPython -m pip install --upgrade pip setuptools wheel

Write-Step "Installing Python requirements..."
& $venvPython -m pip install --upgrade -r $requirementsPath

if ($Device -eq "cuda") {
    Write-Step "Installing CUDA wheels for torch/torchaudio..."
    & $venvPython -m pip install --upgrade --force-reinstall --index-url $CudaIndexUrl torch==2.8.0 torchaudio==2.8.0
}

Write-Step "Verifying runtime..."
& $venvPython -c "import whisperx, rapidfuzz, unidecode, torch; print('torch=' + torch.__version__ + ' cuda=' + str(torch.cuda.is_available()))"
& ffmpeg -version *> $null
& ffprobe -version *> $null

Write-Host ""
Write-Host "READY"
Write-Host "python: $venvPython"
Write-Host "example:"
Write-Host "  `"$venvPython`" `"$ProjectDir\\transcribe.py`" --help"
