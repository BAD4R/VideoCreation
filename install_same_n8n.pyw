#!/usr/bin/env python3
"""
Cross-platform GUI installer that recreates the current machine's n8n setup.

Targets detected from this machine:
- Node.js v22.16.0
- npm 10.9.2 (bundled with Node.js v22.16.0)
- n8n 2.7.5
- ~/.n8n/config values currently in use on this machine

Behavior:
- Windows/macOS only
- Skips work that is already done
- On macOS, installs PowerShell if needed and exposes it as `powershell`
- Installs Transcriber requirements only when CUDA is confirmed
"""

from __future__ import annotations

import json
import os
import platform
import queue
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import traceback
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime
from importlib import metadata
from pathlib import Path
from typing import Callable, Iterable, Optional

import tkinter as tk
from tkinter import messagebox, ttk


APP_TITLE = "n8n Clone Installer"
SCRIPT_DIR = Path(__file__).resolve().parent
LOG_PATH = SCRIPT_DIR / "install_same_n8n.log"
HOME_ENV_PATH = Path.home() / ".env"

TARGET_NODE_VERSION = "22.16.0"
TARGET_N8N_VERSION = "2.7.5"
TARGET_N8N_CONFIG = {
    "encryptionKey": "UvRqeEsl9OtYNcAFXFwP7/+NMAp1XAoF",
    "tunnelSubdomain": "zzezceiju2dyrnrhszzyjtye",
}
EMPTY_NODES_PACKAGE = {
    "name": "installed-nodes",
    "private": True,
    "dependencies": {},
}
TARGET_HOME_ENV = {
    "WEBHOOK_URL": "https://p47p8kqx-5678.euw.devtunnels.ms/",
    "NODES_EXCLUDE": '"[]"',
    "N8N_BLOCK_FILE_ACCESS_TO_N8N_FILES": "false",
    "N8N_BLOCK_ENV_ACCESS_IN_NODE": "false",
}

TRANSCRIBER_REQUIREMENTS_CANDIDATES = [
    SCRIPT_DIR / "SCRIPTS" / "AudioScripts" / "Transcriber" / "requirements.txt",
    Path(
        r"C:\Users\V\Desktop\Projects\VideoCreation\SCRIPTS\AudioScripts\Transcriber\requirements.txt"
    ),
]

WINDOWS_NODE_ROOT = Path(os.environ.get("LOCALAPPDATA", Path.home())) / "Programs" / "VideoCreation" / "nodejs"
MAC_NODE_ROOT = Path.home() / ".local" / "share" / "video_creation" / "nodejs"
MAC_PWSH_ROOT = Path.home() / ".local" / "share" / "video_creation" / "powershell"
MAC_LOCAL_BIN = Path.home() / ".local" / "bin"
MAC_NPM_PREFIX = Path.home() / ".npm-global"
MAC_ENV_SCRIPT = Path.home() / ".video_creation_env.sh"
WINDOWS_N8N_HOME_LAUNCHER = Path(os.environ.get("APPDATA", Path.home())) / "npm" / "n8n-from-home.cmd"
WINDOWS_N8N_SCRIPT_DIR_LAUNCHER = SCRIPT_DIR / "Start n8n.cmd"
MAC_N8N_HOME_LAUNCHER = MAC_LOCAL_BIN / "n8n-from-home"
MAC_RC_FILES = [
    Path.home() / ".zprofile",
    Path.home() / ".zshrc",
    Path.home() / ".bash_profile",
    Path.home() / ".bashrc",
    Path.home() / ".profile",
]

WM_SETTINGCHANGE = 0x001A
SMTO_ABORTIFHUNG = 0x0002


class InstallError(RuntimeError):
    pass


@dataclass(frozen=True)
class Step:
    title: str
    handler: Callable[[], None]


def normalize_version(raw: str) -> str:
    return raw.strip().lstrip("vV")


def shlex_quote(value: str) -> str:
    if not value:
        return "''"
    if re.fullmatch(r"[A-Za-z0-9_./:=+-]+", value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"


def format_command(command: Iterable[str]) -> str:
    command_list = list(command)
    if os.name == "nt":
        return subprocess.list2cmdline(command_list)
    return " ".join(shlex_quote(part) for part in command_list)


def within_directory(parent: Path, child: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    return True


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="\n",
        delete=False,
        dir=str(path.parent),
    ) as handle:
        handle.write(content)
        temp_path = Path(handle.name)
    temp_path.replace(path)


def safe_extract_zip(archive_path: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            target = destination / member.filename
            if not within_directory(destination, target):
                raise InstallError(f"Refusing to extract zip entry outside destination: {member.filename}")
        archive.extractall(destination)


def safe_extract_tar(archive_path: Path, destination: Path) -> None:
    with tarfile.open(archive_path, "r:*") as archive:
        for member in archive.getmembers():
            target = destination / member.name
            if not within_directory(destination, target):
                raise InstallError(f"Refusing to extract tar entry outside destination: {member.name}")
        archive.extractall(destination)


def parse_simple_requirements(path: Path) -> list[tuple[str, str]]:
    requirements: list[tuple[str, str]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "==" not in line:
            raise InstallError(
                f"Unsupported requirement format in {path}: {raw_line}. "
                "Only exact pins like package==version are supported by this installer."
            )
        package_name, version = line.split("==", 1)
        requirements.append((package_name.strip(), version.strip()))
    return requirements


def first_non_empty_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def normalize_path(value: str) -> str:
    return os.path.normcase(os.path.normpath(os.path.expanduser(os.path.expandvars(value))))


def quote_env_value(value: str | Path) -> str:
    text = str(value).replace("\\", "/")
    escaped = text.replace('"', '\\"')
    return f'"{escaped}"'


def upsert_env_file(path: Path, desired: dict[str, str], ordered_keys: list[str]) -> bool:
    original_text = path.read_text(encoding="utf-8") if path.exists() else ""
    original_lines = original_text.splitlines()
    result_lines: list[str] = []
    seen_desired: set[str] = set()

    for line in original_lines:
        match = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$", line)
        if not match:
            result_lines.append(line)
            continue

        key = match.group(1)
        if key in desired:
            if key not in seen_desired:
                result_lines.append(f"{key}={desired[key]}")
                seen_desired.add(key)
            continue

        result_lines.append(line)

    missing_keys = [key for key in ordered_keys if key in desired and key not in seen_desired]
    if missing_keys:
        while result_lines and not result_lines[-1].strip():
            result_lines.pop()
        if result_lines:
            result_lines.append("")
        for index, key in enumerate(missing_keys):
            if key == "CHANNELS_FOLDER_PATH" and result_lines and result_lines[-1].strip():
                result_lines.append("")
            result_lines.append(f"{key}={desired[key]}")
            if index == len(missing_keys) - 1:
                continue

    new_text = "\n".join(result_lines).rstrip() + "\n"
    if new_text == original_text:
        return False

    if path.exists():
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = path.with_name(f"{path.name}.bak_{timestamp}")
        shutil.copy2(path, backup_path)
    atomic_write_text(path, new_text)
    return True


class Installer:
    def __init__(self, logger: Callable[[str], None]) -> None:
        self.log = logger
        self.system = platform.system()
        self.machine = platform.machine().lower()
        self.python_executable = self._detect_python_executable()
        self._unix_path_entries: set[str] = set()
        self.log_file = LOG_PATH
        self.portable_node_dir: Optional[Path] = None
        self.portable_node_bin_dir: Optional[Path] = None

        if self.system not in {"Windows", "Darwin"}:
            raise InstallError(f"Unsupported OS: {self.system}. This installer supports only Windows and macOS.")

    def run(self) -> None:
        self.log(f"Log file: {self.log_file}")
        self.log(f"Detected OS: {self.system}")
        self.log(f"Detected architecture: {self.machine}")
        self.log(f"Python executable: {self.python_executable}")

        steps = [
            Step("Prepare runtime and PATH", self.prepare_runtime),
            Step(f"Ensure Node.js v{TARGET_NODE_VERSION}", self.ensure_node),
            Step(f"Ensure n8n {TARGET_N8N_VERSION}", self.ensure_n8n),
            Step("Ensure n8n config", self.ensure_n8n_config),
            Step("Ensure n8n home .env", self.ensure_n8n_home_env),
            Step("Ensure PowerShell alias on macOS", self.ensure_macos_powershell),
            Step("Ensure CUDA requirements", self.ensure_cuda_requirements),
        ]

        total = len(steps)
        for index, step in enumerate(steps, start=1):
            self.log("")
            self.log(f"[{index}/{total}] {step.title}")
            step.handler()

        self.log("")
        self.log("Installation flow finished successfully.")

    def prepare_runtime(self) -> None:
        if self.system == "Windows":
            appdata_npm = Path(os.environ.get("APPDATA", Path.home())) / "npm"
            self.ensure_windows_user_path(appdata_npm)
            self.log(f"Ensured user PATH contains: {appdata_npm}")
        else:
            self.ensure_unix_path(MAC_LOCAL_BIN)
            self.ensure_unix_path(MAC_NPM_PREFIX / "bin")
            self.log(f"Ensured shell PATH bootstrap file: {MAC_ENV_SCRIPT}")

    def ensure_node(self) -> None:
        existing = self.get_command_version("node")
        if existing and normalize_version(existing) == TARGET_NODE_VERSION:
            self.log(f"Node.js already matches target version: {existing}")
        else:
            if existing:
                self.log(f"Different Node.js detected: {existing}. Installing v{TARGET_NODE_VERSION} locally.")
            else:
                self.log(f"Node.js is missing. Installing v{TARGET_NODE_VERSION}.")

            if self.system == "Windows":
                node_dir = self.install_portable_node_windows()
                self.prepend_process_path(node_dir)
                self.log(f"Portable Node.js installed at: {node_dir}")
            else:
                node_bin_dir = self.install_portable_node_macos()
                self.ensure_unix_path(node_bin_dir)
                self.log(f"Portable Node.js installed at: {node_bin_dir.parent}")

        self.verify_required_command("node", TARGET_NODE_VERSION)
        if self.system == "Windows":
            self.ensure_windows_npm_prefix()
            self.ensure_windows_node_command_shims(include_n8n=False)
            self.remove_windows_managed_powershell_wrappers(include_n8n=False)
            self.remove_windows_managed_node_path_entries()
        npm_version = self.get_command_version("npm")
        if not npm_version:
            raise InstallError("npm is not available after Node.js installation.")
        self.log(f"npm detected: {npm_version}")
        if self.system == "Windows":
            npx_version = self.get_command_version("npx")
            if not npx_version:
                raise InstallError("npx is not available after Node.js installation.")
            self.log(f"npx detected: {npx_version}")

        if self.system == "Windows":
            appdata_npm = Path(os.environ.get("APPDATA", Path.home())) / "npm"
            self.ensure_windows_user_path(appdata_npm)
        else:
            self.ensure_mac_npm_prefix()

    def ensure_n8n(self) -> None:
        if self.system == "Windows":
            existing = self.get_windows_installed_n8n_version()
        else:
            existing = self.get_command_version("n8n")
        if existing and normalize_version(existing) == TARGET_N8N_VERSION:
            self.log(f"n8n already matches target version: {existing}")
            if self.system == "Windows":
                self.remove_windows_prefix_n8n_launchers(remove_cmd=False)
                self.ensure_windows_node_command_shims(include_n8n=True)
                self.remove_windows_managed_powershell_wrappers(include_n8n=True)
                self.verify_required_command("n8n", TARGET_N8N_VERSION)
                self.verify_windows_npx_n8n()
            return

        if existing:
            self.log(f"Different n8n detected: {existing}. Installing {TARGET_N8N_VERSION}.")
        else:
            self.log(f"n8n is missing. Installing {TARGET_N8N_VERSION}.")

        if self.system == "Windows":
            self.ensure_windows_npm_prefix()
            self.remove_windows_prefix_n8n_launchers(remove_cmd=True)
        self.run_command(
            ["npm", "install", "-g", f"n8n@{TARGET_N8N_VERSION}"],
            "Installing n8n globally",
        )
        if self.system == "Windows":
            self.remove_windows_prefix_n8n_launchers(remove_cmd=False)
            self.ensure_windows_node_command_shims(include_n8n=True)
            self.remove_windows_managed_powershell_wrappers(include_n8n=True)
        self.verify_required_command("n8n", TARGET_N8N_VERSION)
        if self.system == "Windows":
            self.verify_windows_npx_n8n()

    def ensure_n8n_config(self) -> None:
        n8n_dir = Path.home() / ".n8n"
        n8n_dir.mkdir(parents=True, exist_ok=True)

        nodes_package = n8n_dir / "nodes" / "package.json"
        if not nodes_package.exists():
            content = json.dumps(EMPTY_NODES_PACKAGE, indent=2) + "\n"
            atomic_write_text(nodes_package, content)
            self.log(f"Created missing n8n nodes manifest: {nodes_package}")
        else:
            self.log(f"n8n nodes manifest already exists: {nodes_package}")

        config_path = n8n_dir / "config"
        current_data: dict[str, object] = {}
        changed = False

        if config_path.exists():
            try:
                raw = config_path.read_text(encoding="utf-8")
                loaded = json.loads(raw)
                if isinstance(loaded, dict):
                    current_data = loaded
                else:
                    self.backup_file(config_path)
                    self.log(f"Existing config is not a JSON object, backing it up: {config_path}")
                    current_data = {}
                    changed = True
            except Exception:
                self.backup_file(config_path)
                self.log(f"Existing config could not be parsed, backing it up: {config_path}")
                current_data = {}
                changed = True

        for key, value in TARGET_N8N_CONFIG.items():
            if current_data.get(key) != value:
                current_data[key] = value
                changed = True

        if changed or not config_path.exists():
            content = json.dumps(current_data, indent="\t", ensure_ascii=False) + "\n"
            atomic_write_text(config_path, content)
            self.log(f"Applied target n8n config to: {config_path}")
        else:
            self.log("n8n config already matches the target values.")

    def ensure_n8n_home_env(self) -> None:
        desired_env = self.build_target_home_env()
        channels_dir = self.channels_folder_path()
        channels_dir.mkdir(parents=True, exist_ok=True)
        self.log(f"Ensured channels folder exists: {channels_dir}")
        self.log("n8n loads `.env` from its current working directory, so this installer keeps the file in the user's home directory to match the current machine.")

        changed = upsert_env_file(
            HOME_ENV_PATH,
            desired_env,
            ordered_keys=[
                "WEBHOOK_URL",
                "NODES_EXCLUDE",
                "N8N_BLOCK_FILE_ACCESS_TO_N8N_FILES",
                "N8N_RESTRICT_FILE_ACCESS_TO",
                "N8N_BLOCK_ENV_ACCESS_IN_NODE",
                "CHANNELS_FOLDER_PATH",
            ],
        )
        if changed:
            self.log(f"Applied n8n home .env values to: {HOME_ENV_PATH}")
        else:
            self.log(f"n8n home .env already matches target values: {HOME_ENV_PATH}")

        self.ensure_n8n_home_launcher()

    def ensure_macos_powershell(self) -> None:
        if self.system != "Darwin":
            self.log("Skipping macOS PowerShell step on Windows.")
            return

        powershell_path = shutil.which("powershell")
        if powershell_path:
            self.log(f"`powershell` is already available: {powershell_path}")
            return

        pwsh_path = shutil.which("pwsh")
        if pwsh_path:
            self.log(f"`pwsh` already exists: {pwsh_path}")
        else:
            self.log("PowerShell is missing on macOS. Installing a portable build.")
            pwsh_path = str(self.install_portable_powershell_macos())

        self.ensure_unix_path(MAC_LOCAL_BIN)
        wrapper_path = MAC_LOCAL_BIN / "powershell"
        wrapper_content = "\n".join(
            [
                "#!/usr/bin/env python3",
                "import os",
                "import re",
                "import sys",
                "",
                f"PW_SH = {pwsh_path!r}",
                "",
                "def quote(arg: str) -> str:",
                '    if re.fullmatch(r"[-A-Za-z0-9_./:=+\\\\]+", arg):',
                "        return arg",
                "    return \"'\" + arg.replace(\"'\", \"''\") + \"'\"",
                "",
                "args = sys.argv[1:]",
                "if not args:",
                "    os.execv(PW_SH, [PW_SH])",
                "if args[0].startswith('-'):",
                "    os.execv(PW_SH, [PW_SH, *args])",
                "if len(args) == 1:",
                "    os.execv(PW_SH, [PW_SH, '-NoLogo', '-Command', args[0]])",
                "command = ' '.join(quote(arg) for arg in args)",
                "os.execv(PW_SH, [PW_SH, '-NoLogo', '-Command', command])",
                "",
            ]
        )

        current_wrapper = wrapper_path.read_text(encoding="utf-8") if wrapper_path.exists() else None
        if current_wrapper != wrapper_content:
            if wrapper_path.exists():
                self.backup_file(wrapper_path)
            atomic_write_text(wrapper_path, wrapper_content)
            os.chmod(wrapper_path, 0o755)
            self.log(f"Created macOS wrapper command: {wrapper_path}")
        else:
            self.log(f"Wrapper already up to date: {wrapper_path}")

        verify = self.run_quiet(
            [str(wrapper_path), "-NoLogo", "-NoProfile", "-Command", "$PSVersionTable.PSVersion.ToString()"],
            check=False,
        )
        if verify.returncode != 0:
            raise InstallError("The macOS `powershell` wrapper was created but could not be executed.")
        self.log(f"`powershell` wrapper verified. PowerShell version: {first_non_empty_line(verify.stdout)}")

    def ensure_n8n_home_launcher(self) -> None:
        if self.system == "Windows":
            launcher_targets = [
                WINDOWS_N8N_HOME_LAUNCHER,
                WINDOWS_N8N_SCRIPT_DIR_LAUNCHER,
            ]
            launcher_content = "\r\n".join(
                [
                    "@echo off",
                    "cd /d %USERPROFILE%",
                    "n8n %*",
                    "",
                ]
            )
        else:
            self.ensure_unix_path(MAC_LOCAL_BIN)
            launcher_targets = [MAC_N8N_HOME_LAUNCHER]
            launcher_content = "\n".join(
                [
                    "#!/bin/sh",
                    'cd "$HOME" || exit 1',
                    'exec n8n "$@"',
                    "",
                ]
            )

        for launcher_path in launcher_targets:
            launcher_path.parent.mkdir(parents=True, exist_ok=True)
            current = launcher_path.read_text(encoding="utf-8") if launcher_path.exists() else None
            if current != launcher_content:
                if launcher_path.exists():
                    self.backup_file(launcher_path)
                atomic_write_text(launcher_path, launcher_content)
                if self.system != "Windows":
                    os.chmod(launcher_path, 0o755)
                self.log(f"Created launcher that starts n8n from the home directory: {launcher_path}")
            else:
                self.log(f"Home-directory launcher already up to date: {launcher_path}")

    def ensure_cuda_requirements(self) -> None:
        requirements_path = self.find_transcriber_requirements()
        if not requirements_path:
            self.log("Transcriber requirements.txt was not found. Skipping CUDA requirements step.")
            return

        if not self.has_cuda():
            self.log("CUDA was not confirmed on this device. Skipping Transcriber requirements.")
            return

        self.ensure_pip()
        if self.requirements_are_satisfied(requirements_path):
            self.log("CUDA requirements are already installed with the requested versions.")
            return

        self.run_command(
            [self.python_executable, "-m", "pip", "install", "-r", str(requirements_path)],
            f"Installing CUDA requirements from {requirements_path}",
        )

        if not self.requirements_are_satisfied(requirements_path):
            raise InstallError("pip finished, but the Transcriber requirements still do not match the requested versions.")

        self.log("CUDA requirements installed successfully.")

    def ensure_mac_npm_prefix(self) -> None:
        MAC_NPM_PREFIX.mkdir(parents=True, exist_ok=True)
        self.ensure_unix_path(MAC_NPM_PREFIX / "bin")

        current = self.run_quiet(["npm", "config", "get", "prefix"], check=False)
        current_prefix = current.stdout.strip() if current.returncode == 0 else ""
        if current_prefix and Path(current_prefix).expanduser().resolve() == MAC_NPM_PREFIX.resolve():
            self.log(f"npm global prefix already set to: {MAC_NPM_PREFIX}")
            return

        self.run_command(
            ["npm", "config", "set", "prefix", str(MAC_NPM_PREFIX)],
            f"Setting npm global prefix to {MAC_NPM_PREFIX}",
        )
        self.log(f"npm global prefix set to: {MAC_NPM_PREFIX}")

    def ensure_windows_npm_prefix(self) -> None:
        if self.system != "Windows":
            return

        prefix_dir = self.windows_npm_prefix_dir()
        prefix_dir.mkdir(parents=True, exist_ok=True)
        self.ensure_windows_user_path(prefix_dir)

        current = self.run_quiet(["npm", "config", "get", "prefix"], check=False)
        current_prefix = current.stdout.strip() if current.returncode == 0 else ""
        expected = prefix_dir.resolve()
        if current_prefix:
            try:
                current_resolved = Path(current_prefix).expanduser().resolve()
            except Exception:
                current_resolved = Path(current_prefix)
            if normalize_path(str(current_resolved)) == normalize_path(str(expected)):
                self.log(f"npm global prefix already set to: {prefix_dir}")
                return

        self.run_command(
            ["npm", "config", "set", "prefix", str(prefix_dir)],
            f"Setting npm global prefix to {prefix_dir}",
        )
        self.log(f"npm global prefix set to: {prefix_dir}")

    def build_target_home_env(self) -> dict[str, str]:
        target = dict(TARGET_HOME_ENV)
        target["N8N_RESTRICT_FILE_ACCESS_TO"] = '"C:"' if self.system == "Windows" else '"/"'
        target["CHANNELS_FOLDER_PATH"] = quote_env_value(self.channels_folder_path())
        return target

    def windows_npm_prefix_dir(self) -> Path:
        return Path(os.environ.get("APPDATA", Path.home())) / "npm"

    def windows_npm_global_modules_dir(self) -> Path:
        return self.windows_npm_prefix_dir() / "node_modules"

    def windows_n8n_bin_script(self) -> Path:
        return self.windows_npm_global_modules_dir() / "n8n" / "bin" / "n8n"

    def windows_n8n_package_dirs(self) -> list[Path]:
        package_dirs = [self.windows_npm_global_modules_dir() / "n8n"]

        portable_dir = self.portable_node_dir or self.detect_portable_node_windows_dir()
        if portable_dir:
            package_dirs.append(portable_dir / "node_modules" / "n8n")

        unique_dirs: list[Path] = []
        seen: set[str] = set()
        for package_dir in package_dirs:
            normalized_package_dir = normalize_path(str(package_dir))
            if normalized_package_dir in seen:
                continue
            seen.add(normalized_package_dir)
            unique_dirs.append(package_dir)
        return unique_dirs

    def get_windows_installed_n8n_version(self) -> Optional[str]:
        for package_dir in self.windows_n8n_package_dirs():
            package_json = package_dir / "package.json"
            if not package_json.exists():
                continue
            try:
                payload = json.loads(package_json.read_text(encoding="utf-8"))
            except Exception:
                continue
            version = payload.get("version")
            if isinstance(version, str) and version.strip():
                return version.strip()
        return None

    def windows_n8n_cmd_target(self) -> Optional[Path]:
        candidates = []

        for package_dir in self.windows_n8n_package_dirs():
            candidates.extend(
                [
                    package_dir / "bin" / "n8n.cmd",
                    package_dir / "bin" / "n8n",
                ]
            )

        portable_dir = self.portable_node_dir or self.detect_portable_node_windows_dir()
        if portable_dir:
            candidates.extend(
                [
                    portable_dir / "n8n.cmd",
                    portable_dir / "n8n",
                ]
            )

        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def remove_windows_prefix_n8n_launchers(self, *, remove_cmd: bool) -> None:
        if self.system != "Windows":
            return

        prefix_dir = self.windows_npm_prefix_dir()
        launcher_paths = [
            prefix_dir / "n8n",
            prefix_dir / "n8n.ps1",
        ]
        if remove_cmd:
            launcher_paths.insert(0, prefix_dir / "n8n.cmd")

        for launcher_path in launcher_paths:
            if not launcher_path.exists():
                continue
            self.backup_file(launcher_path)
            launcher_path.unlink()
            self.log(f"Removed conflicting n8n launcher: {launcher_path}")

    def remove_windows_managed_powershell_wrappers(self, *, include_n8n: bool) -> None:
        if self.system != "Windows":
            return

        portable_dir = self.portable_node_dir or self.detect_portable_node_windows_dir()
        wrapper_paths: list[Path] = []

        if portable_dir:
            wrapper_paths.extend(
                [
                    portable_dir / "npm.ps1",
                    portable_dir / "npx.ps1",
                    portable_dir / "corepack.ps1",
                ]
            )
            if include_n8n:
                wrapper_paths.append(portable_dir / "n8n.ps1")

        prefix_dir = self.windows_npm_prefix_dir()
        if include_n8n:
            wrapper_paths.append(prefix_dir / "n8n.ps1")

        seen: set[str] = set()
        for wrapper_path in wrapper_paths:
            normalized_wrapper_path = normalize_path(str(wrapper_path))
            if normalized_wrapper_path in seen:
                continue
            seen.add(normalized_wrapper_path)
            if not wrapper_path.exists():
                continue
            self.backup_file(wrapper_path)
            wrapper_path.unlink()
            self.log(f"Removed managed PowerShell wrapper: {wrapper_path}")

    def channels_folder_path(self) -> Path:
        return self.desktop_path() / "Channels"

    def desktop_path(self) -> Path:
        return Path.home() / "Desktop"

    def install_portable_node_windows(self) -> Path:
        arch = self.windows_node_arch()
        dist_name = f"node-v{TARGET_NODE_VERSION}-win-{arch}"
        install_dir = WINDOWS_NODE_ROOT / dist_name
        node_exe = install_dir / "node.exe"

        if node_exe.exists():
            self.portable_node_dir = install_dir
            self.portable_node_bin_dir = install_dir
            return install_dir

        WINDOWS_NODE_ROOT.mkdir(parents=True, exist_ok=True)
        url = f"https://nodejs.org/dist/v{TARGET_NODE_VERSION}/{dist_name}.zip"
        self.download_and_extract_zip(url, WINDOWS_NODE_ROOT, install_dir)
        if not node_exe.exists():
            raise InstallError(f"Portable Node.js did not extract correctly: {node_exe}")
        self.portable_node_dir = install_dir
        self.portable_node_bin_dir = install_dir
        return install_dir

    def install_portable_node_macos(self) -> Path:
        arch = self.macos_node_arch()
        dist_name = f"node-v{TARGET_NODE_VERSION}-darwin-{arch}"
        install_dir = MAC_NODE_ROOT / dist_name
        node_binary = install_dir / "bin" / "node"

        if node_binary.exists():
            self.portable_node_dir = install_dir
            self.portable_node_bin_dir = install_dir / "bin"
            return install_dir / "bin"

        MAC_NODE_ROOT.mkdir(parents=True, exist_ok=True)
        url = f"https://nodejs.org/dist/v{TARGET_NODE_VERSION}/{dist_name}.tar.gz"
        self.download_and_extract_tar(url, MAC_NODE_ROOT, install_dir)
        if not node_binary.exists():
            raise InstallError(f"Portable Node.js did not extract correctly: {node_binary}")
        self.portable_node_dir = install_dir
        self.portable_node_bin_dir = install_dir / "bin"
        return install_dir / "bin"

    def install_portable_powershell_macos(self) -> Path:
        tag_name, download_url, asset_name = self.lookup_latest_powershell_asset()
        install_dir = MAC_PWSH_ROOT / asset_name.removesuffix(".tar.gz")
        pwsh_binary = self.find_pwsh_binary(install_dir)

        if pwsh_binary.exists():
            self.ensure_unix_path(pwsh_binary.parent)
            return pwsh_binary

        MAC_PWSH_ROOT.mkdir(parents=True, exist_ok=True)
        self.download_and_extract_tar(download_url, install_dir, install_dir, flatten=True)
        pwsh_binary = self.find_pwsh_binary(install_dir)

        if not pwsh_binary.exists():
            raise InstallError(f"Portable PowerShell did not extract correctly: {pwsh_binary}")

        os.chmod(pwsh_binary, 0o755)
        self.ensure_unix_path(pwsh_binary.parent)
        self.log(f"Portable PowerShell {tag_name} installed at: {pwsh_binary.parent}")
        return pwsh_binary

    def lookup_latest_powershell_asset(self) -> tuple[str, str, str]:
        arch = "arm64" if self.machine in {"arm64", "aarch64"} else "x64"
        url = "https://api.github.com/repos/PowerShell/PowerShell/releases/latest"
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": "VideoCreation-n8n-installer",
            },
        )
        self.log("Querying the latest official PowerShell release metadata from GitHub.")
        with urllib.request.urlopen(request) as response:
            payload = json.loads(response.read().decode("utf-8"))

        assets = payload.get("assets", [])
        pattern = re.compile(rf"^powershell-\d+\.\d+\.\d+-osx-{arch}\.tar\.gz$")
        for asset in assets:
            name = asset.get("name", "")
            if pattern.match(name):
                return payload.get("tag_name", "unknown"), asset["browser_download_url"], name

        raise InstallError(f"Could not find an official macOS PowerShell tarball for architecture: {arch}")

    def download_and_extract_zip(self, url: str, destination_root: Path, expected_dir: Path) -> None:
        destination_root.mkdir(parents=True, exist_ok=True)
        self.clear_managed_dir(expected_dir, destination_root)
        archive_path = self.download_file(url)
        try:
            self.log(f"Extracting archive to: {destination_root}")
            safe_extract_zip(archive_path, destination_root)
        finally:
            archive_path.unlink(missing_ok=True)

    def download_and_extract_tar(
        self,
        url: str,
        destination_root: Path,
        expected_dir: Path,
        *,
        flatten: bool = False,
    ) -> None:
        destination_root.mkdir(parents=True, exist_ok=True)
        self.clear_managed_dir(expected_dir, destination_root)
        archive_path = self.download_file(url)
        try:
            self.log(f"Extracting archive to: {destination_root}")

            if flatten:
                expected_dir.mkdir(parents=True, exist_ok=True)
                safe_extract_tar(archive_path, expected_dir)
            else:
                safe_extract_tar(archive_path, destination_root)
        finally:
            archive_path.unlink(missing_ok=True)

    def download_file(self, url: str) -> Path:
        self.log(f"Downloading: {url}")
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "VideoCreation-n8n-installer"},
        )
        with urllib.request.urlopen(request) as response:
            total_size = int(response.headers.get("Content-Length", "0") or "0")
            suffix = Path(urllib.parse.urlparse(url).path).suffix or ".bin"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
                temp_path = Path(handle.name)
                downloaded = 0
                next_report = 5
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = int(downloaded * 100 / total_size)
                        if percent >= next_report:
                            self.log(f"Downloaded {percent}%")
                            next_report = min(100, next_report + 10)

        self.log(f"Downloaded to temporary file: {temp_path}")
        return temp_path

    def ensure_pip(self) -> None:
        pip_check = self.run_quiet([self.python_executable, "-m", "pip", "--version"], check=False)
        if pip_check.returncode == 0:
            self.log(first_non_empty_line(pip_check.stdout))
            return

        self.run_command(
            [self.python_executable, "-m", "ensurepip", "--upgrade"],
            "Installing pip into the current Python runtime",
        )

    def requirements_are_satisfied(self, requirements_path: Path) -> bool:
        parsed = parse_simple_requirements(requirements_path)
        mismatches: list[str] = []
        for package_name, expected_version in parsed:
            try:
                installed_version = metadata.version(package_name)
            except metadata.PackageNotFoundError:
                mismatches.append(f"{package_name}=={expected_version} (missing)")
                continue

            if installed_version != expected_version:
                mismatches.append(
                    f"{package_name}=={expected_version} (installed: {installed_version})"
                )

        if mismatches:
            self.log("Requirement mismatch detected:")
            for entry in mismatches:
                self.log(f"  - {entry}")
            return False
        return True

    def has_cuda(self) -> bool:
        nvidia_smi = shutil.which("nvidia-smi")
        if nvidia_smi:
            result = self.run_quiet([nvidia_smi, "-L"], check=False)
            if result.returncode == 0 and "GPU " in result.stdout:
                self.log("CUDA-capable GPU confirmed through nvidia-smi.")
                self.log(first_non_empty_line(result.stdout))
                return True

        if self.system == "Windows":
            probe = self.run_quiet(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    "(Get-CimInstance Win32_VideoController | Select-Object -ExpandProperty Name) -join \"`n\"",
                ],
                check=False,
            )
            if probe.returncode == 0 and "nvidia" in probe.stdout.lower():
                self.log("An NVIDIA GPU was detected, but CUDA tooling is not ready yet (nvidia-smi failed).")
            else:
                self.log("No CUDA-capable GPU was confirmed on Windows.")
        else:
            self.log("No CUDA-capable GPU was confirmed on macOS.")

        return False

    def find_transcriber_requirements(self) -> Optional[Path]:
        for candidate in TRANSCRIBER_REQUIREMENTS_CANDIDATES:
            if candidate.exists():
                self.log(f"Using requirements file: {candidate}")
                return candidate
        return None

    def find_pwsh_binary(self, install_dir: Path) -> Path:
        direct = install_dir / "pwsh"
        if direct.exists():
            return direct
        matches = [path for path in install_dir.rglob("pwsh") if path.is_file()]
        if len(matches) == 1:
            return matches[0]
        return direct

    def verify_required_command(self, executable: str, expected_version: str) -> None:
        actual = self.get_command_version(executable)
        if not actual:
            raise InstallError(f"{executable} is still not available after installation.")
        if normalize_version(actual) != normalize_version(expected_version):
            raise InstallError(
                f"{executable} version mismatch. Expected {expected_version}, got {actual}."
            )
        self.log(f"{executable} verified: {actual}")

    def prepare_command(self, command: list[str]) -> list[str]:
        if not command:
            return command
        resolved = self.resolve_command_path(command[0])
        if not resolved:
            return command
        return [resolved, *command[1:]]

    def resolve_command_path(self, executable: str) -> Optional[str]:
        direct_path = Path(executable)
        if any(sep in executable for sep in ("\\", "/")) or direct_path.drive:
            return str(direct_path) if direct_path.exists() else None

        if self.system == "Windows":
            appdata_npm = Path(os.environ.get("APPDATA", Path.home())) / "npm"
            preferred_wrappers = {
                "node": appdata_npm / "node.cmd",
                "npm": appdata_npm / "npm.cmd",
                "npx": appdata_npm / "npx.cmd",
                "corepack": appdata_npm / "corepack.cmd",
                "n8n": appdata_npm / "n8n.cmd",
            }
            preferred = preferred_wrappers.get(executable.lower())
            if preferred and preferred.exists():
                return str(preferred)

            portable_dir = self.portable_node_dir or self.detect_portable_node_windows_dir()
            if portable_dir:
                windows_binaries = {
                    "node": portable_dir / "node.exe",
                    "npm": portable_dir / "npm.cmd",
                    "npx": portable_dir / "npx.cmd",
                    "corepack": portable_dir / "corepack.cmd",
                }
                candidate = windows_binaries.get(executable.lower())
                if candidate and candidate.exists():
                    return str(candidate)

        if self.system == "Darwin":
            portable_bin = self.portable_node_bin_dir or self.detect_portable_node_macos_bin_dir()
            if portable_bin:
                unix_binaries = {
                    "node": portable_bin / "node",
                    "npm": portable_bin / "npm",
                    "npx": portable_bin / "npx",
                    "corepack": portable_bin / "corepack",
                }
                candidate = unix_binaries.get(executable.lower())
                if candidate and candidate.exists():
                    return str(candidate)

            mac_candidates = {
                "n8n": MAC_NPM_PREFIX / "bin" / "n8n",
                "powershell": MAC_LOCAL_BIN / "powershell",
            }
            candidate = mac_candidates.get(executable.lower())
            if candidate and candidate.exists():
                return str(candidate)

        direct = shutil.which(executable)
        if direct:
            return direct

        return None

    def detect_portable_node_windows_dir(self) -> Optional[Path]:
        candidate = WINDOWS_NODE_ROOT / f"node-v{TARGET_NODE_VERSION}-win-{self.windows_node_arch()}"
        if (candidate / "node.exe").exists():
            self.portable_node_dir = candidate
            self.portable_node_bin_dir = candidate
            return candidate
        return None

    def detect_portable_node_macos_bin_dir(self) -> Optional[Path]:
        candidate = MAC_NODE_ROOT / f"node-v{TARGET_NODE_VERSION}-darwin-{self.macos_node_arch()}" / "bin"
        if (candidate / "node").exists():
            self.portable_node_dir = candidate.parent
            self.portable_node_bin_dir = candidate
            return candidate
        return None

    def ensure_windows_node_command_shims(self, *, include_n8n: bool) -> None:
        if self.system != "Windows":
            return

        portable_dir = self.portable_node_dir or self.detect_portable_node_windows_dir()
        if not portable_dir:
            self.log("Portable Node.js directory was not detected, skipping Windows command shims.")
            return

        shims_dir = Path(os.environ.get("APPDATA", Path.home())) / "npm"
        shims_dir.mkdir(parents=True, exist_ok=True)
        self.ensure_windows_user_path(shims_dir)

        node_exe = portable_dir / "node.exe"
        command_targets = {
            "node.cmd": node_exe,
            "npm.cmd": portable_dir / "npm.cmd",
            "npx.cmd": portable_dir / "npx.cmd",
            "corepack.cmd": portable_dir / "corepack.cmd",
        }

        for shim_name, target_path in command_targets.items():
            if not target_path.exists():
                self.log(f"Skipping shim {shim_name}: target not found at {target_path}")
                continue

            if shim_name == "npx.cmd":
                shim_content = "\r\n".join(
                    [
                        "@echo off",
                        f"if /I \"%~1\"==\"n8n\" (",
                        "  shift",
                        f"  call \"{shims_dir / 'n8n.cmd'}\" %*",
                        "  exit /b %ERRORLEVEL%",
                        ")",
                        f"call \"{target_path}\" %*",
                        "",
                    ]
                )
            elif target_path.suffix.lower() == ".exe":
                shim_content = "\r\n".join(
                    [
                        "@echo off",
                        f"\"{target_path}\" %*",
                        "",
                    ]
                )
            else:
                shim_content = "\r\n".join(
                    [
                        "@echo off",
                        f"call \"{target_path}\" %*",
                        "",
                    ]
                )

            self.write_windows_cmd_wrapper(shims_dir / shim_name, shim_content)

        if not include_n8n:
            return

        n8n_target = self.windows_n8n_cmd_target()
        if n8n_target and n8n_target.exists():
            if n8n_target.suffix.lower() == ".cmd":
                n8n_content = "\r\n".join(
                    [
                        "@echo off",
                        "cd /d %USERPROFILE%",
                        f"call \"{n8n_target}\" %*",
                        "",
                    ]
                )
            elif node_exe.exists():
                n8n_content = "\r\n".join(
                    [
                        "@echo off",
                        "cd /d %USERPROFILE%",
                        f"\"{node_exe}\" \"{n8n_target}\" %*",
                        "",
                    ]
                )
            else:
                n8n_content = ""

            if n8n_content:
                self.write_windows_cmd_wrapper(shims_dir / "n8n.cmd", n8n_content)
            else:
                self.log(f"Skipping shim n8n.cmd: node.exe is missing for target {n8n_target}")
        else:
            self.log("Skipping shim n8n.cmd: target was not found in the npm prefix or portable Node.js directory")

    def write_windows_cmd_wrapper(self, shim_path: Path, shim_content: str) -> None:
        current = shim_path.read_text(encoding="utf-8") if shim_path.exists() else None
        normalized_content = shim_content.replace("\r\n", "\n")
        if current == normalized_content:
            self.log(f"Windows shim already up to date: {shim_path}")
            return

        if shim_path.exists():
            self.backup_file(shim_path)
        atomic_write_text(shim_path, normalized_content)
        self.log(f"Created Windows shim: {shim_path}")

    def verify_windows_npx_n8n(self) -> None:
        if self.system != "Windows":
            return

        n8n_result = self.run_quiet(["n8n", "--version"], check=False)
        if n8n_result.returncode != 0:
            raise InstallError(
                "n8n is not available after installation.\n"
                f"STDOUT:\n{n8n_result.stdout}\nSTDERR:\n{n8n_result.stderr}"
            )
        self.log(f"n8n launcher verified: {first_non_empty_line((n8n_result.stdout or '') + (n8n_result.stderr or ''))}")

        result = self.run_quiet(["npx.cmd", "--version"], check=False)
        if result.returncode != 0:
            raise InstallError(
                "npx.cmd is not available after installation.\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )
        self.log(f"npx.cmd verified: {first_non_empty_line((result.stdout or '') + (result.stderr or ''))}")

    def get_command_version(self, executable: str) -> Optional[str]:
        resolved = self.resolve_command_path(executable)
        if not resolved:
            return None
        result = self.run_quiet([resolved, "--version"], check=False)
        if result.returncode != 0:
            return None
        output = (result.stdout or "") + "\n" + (result.stderr or "")
        version = first_non_empty_line(output)
        return version or None

    def run_command(self, command: list[str], description: str) -> None:
        prepared_command = self.prepare_command(command)
        self.log(description)
        self.log(f"$ {format_command(prepared_command)}")

        try:
            process = subprocess.Popen(
                prepared_command,
                cwd=str(SCRIPT_DIR),
                env=os.environ.copy(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except FileNotFoundError as exc:
            raise InstallError(f"Command not found: {prepared_command[0]}") from exc

        assert process.stdout is not None
        for line in process.stdout:
            self.log(line.rstrip())

        return_code = process.wait()
        if return_code != 0:
            raise InstallError(f"Command failed with exit code {return_code}: {format_command(prepared_command)}")

    def run_quiet(self, command: list[str], *, check: bool) -> subprocess.CompletedProcess[str]:
        prepared_command = self.prepare_command(command)
        try:
            result = subprocess.run(
                prepared_command,
                cwd=str(SCRIPT_DIR),
                env=os.environ.copy(),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except FileNotFoundError as exc:
            if check:
                raise InstallError(f"Command not found: {prepared_command[0]}") from exc
            result = subprocess.CompletedProcess(
                prepared_command,
                127,
                stdout="",
                stderr=str(exc),
            )
        if check and result.returncode != 0:
            raise InstallError(
                f"Command failed with exit code {result.returncode}: {format_command(prepared_command)}\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )
        return result

    def ensure_windows_user_path(self, entry: Path) -> None:
        entry = entry.expanduser().resolve()
        self.prepend_process_path(entry)

        import winreg

        try:
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                "Environment",
                0,
                winreg.KEY_READ | winreg.KEY_SET_VALUE,
            ) as key:
                try:
                    current_path, _ = winreg.QueryValueEx(key, "Path")
                except FileNotFoundError:
                    current_path = ""

                normalized_entry = normalize_path(str(entry))
                parts = [part for part in current_path.split(os.pathsep) if part]
                reordered_parts = [part for part in parts if normalize_path(part) != normalized_entry]
                reordered_parts.insert(0, str(entry))
                new_value = os.pathsep.join(reordered_parts)
                if new_value != current_path:
                    winreg.SetValueEx(key, "Path", 0, winreg.REG_EXPAND_SZ, new_value)
                    self.broadcast_environment_change()
        except PermissionError as exc:
            raise InstallError(f"Unable to update the Windows user PATH: {exc}") from exc

    def remove_windows_user_path(self, entry: Path) -> bool:
        if self.system != "Windows":
            return False

        entry = entry.expanduser().resolve()

        import winreg

        try:
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                "Environment",
                0,
                winreg.KEY_READ | winreg.KEY_SET_VALUE,
            ) as key:
                try:
                    current_path, _ = winreg.QueryValueEx(key, "Path")
                except FileNotFoundError:
                    current_path = ""

                normalized_entry = normalize_path(str(entry))
                parts = [part for part in current_path.split(os.pathsep) if part]
                filtered_parts = [part for part in parts if normalize_path(part) != normalized_entry]
                new_value = os.pathsep.join(filtered_parts)
                if new_value == current_path:
                    return False

                winreg.SetValueEx(key, "Path", 0, winreg.REG_EXPAND_SZ, new_value)
                self.broadcast_environment_change()
                return True
        except PermissionError as exc:
            raise InstallError(f"Unable to update the Windows user PATH: {exc}") from exc

    def remove_windows_managed_node_path_entries(self) -> None:
        if self.system != "Windows":
            return

        candidates: list[Path] = []
        portable_dir = self.portable_node_dir or self.detect_portable_node_windows_dir()
        if portable_dir:
            candidates.append(portable_dir)

        if WINDOWS_NODE_ROOT.exists():
            candidates.extend(
                path for path in WINDOWS_NODE_ROOT.iterdir() if path.is_dir()
            )

        removed_any = False
        seen: set[str] = set()
        for candidate in candidates:
            normalized_candidate = normalize_path(str(candidate))
            if normalized_candidate in seen:
                continue
            seen.add(normalized_candidate)
            if self.remove_windows_user_path(candidate):
                self.log(f"Removed managed Node.js directory from user PATH: {candidate}")
                removed_any = True

        if not removed_any:
            self.log("Managed Node.js directory is not present in the user PATH.")

    def ensure_unix_path(self, entry: Path) -> None:
        entry = entry.expanduser().resolve()
        self.prepend_process_path(entry)
        self._unix_path_entries.add(self.to_shell_path(entry))
        self.rewrite_unix_env_script()
        self.ensure_unix_shell_sources_env_script()

    def prepend_process_path(self, entry: Path) -> None:
        entry_str = str(entry)
        path_parts = [part for part in os.environ.get("PATH", "").split(os.pathsep) if part]
        normalized = {normalize_path(part) for part in path_parts}
        if normalize_path(entry_str) not in normalized:
            os.environ["PATH"] = entry_str + os.pathsep + os.environ.get("PATH", "")

    def rewrite_unix_env_script(self) -> None:
        lines = [
            "#!/bin/sh",
            "# Managed by install_same_n8n.pyw",
            "path_prepend() {",
            '  case ":$PATH:" in',
            '    *":$1:"*) ;;',
            '    *) PATH="$1:$PATH" ;;',
            "  esac",
            "}",
        ]
        for entry in sorted(self._unix_path_entries):
            lines.append(f'path_prepend "{entry}"')
        lines.append("export PATH")
        content = "\n".join(lines) + "\n"
        atomic_write_text(MAC_ENV_SCRIPT, content)
        os.chmod(MAC_ENV_SCRIPT, 0o755)

    def ensure_unix_shell_sources_env_script(self) -> None:
        source_line = '[ -f "$HOME/.video_creation_env.sh" ] && . "$HOME/.video_creation_env.sh"'
        existing = [path for path in MAC_RC_FILES if path.exists()]
        targets = existing if existing else [Path.home() / ".zprofile"]
        for path in targets:
            current = path.read_text(encoding="utf-8") if path.exists() else ""
            if source_line in current:
                continue
            new_content = current.rstrip()
            if new_content:
                new_content += "\n\n"
            new_content += source_line + "\n"
            atomic_write_text(path, new_content)

    def clear_managed_dir(self, target: Path, allowed_root: Path) -> None:
        if not target.exists():
            return
        if not within_directory(allowed_root, target):
            raise InstallError(f"Refusing to remove directory outside the managed root: {target}")
        shutil.rmtree(target)

    def backup_file(self, path: Path) -> None:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = path.with_name(f"{path.name}.bak_{timestamp}")
        shutil.copy2(path, backup_path)
        self.log(f"Backup created: {backup_path}")

    def windows_node_arch(self) -> str:
        if "arm" in self.machine:
            return "arm64"
        return "x64"

    def macos_node_arch(self) -> str:
        if self.machine in {"arm64", "aarch64"}:
            return "arm64"
        return "x64"

    def to_shell_path(self, entry: Path) -> str:
        home_str = str(Path.home())
        entry_str = str(entry)
        if entry_str.startswith(home_str):
            return "$HOME" + entry_str[len(home_str) :]
        return entry_str

    def broadcast_environment_change(self) -> None:
        if self.system != "Windows":
            return

        import ctypes

        ctypes.windll.user32.SendMessageTimeoutW(
            0xFFFF,
            WM_SETTINGCHANGE,
            0,
            "Environment",
            SMTO_ABORTIFHUNG,
            5000,
            None,
        )

    def _detect_python_executable(self) -> str:
        current = Path(sys.executable)
        if current.name.lower() == "pythonw.exe":
            candidate = current.with_name("python.exe")
            if candidate.exists():
                return str(candidate)
        return str(current)


class InstallerApp:
    def __init__(self) -> None:
        self.root = tk.Tk()
        self.root.title(APP_TITLE)
        self.root.geometry("960x620")
        self.root.minsize(860, 520)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self.log_queue: "queue.Queue[tuple[str, str]]" = queue.Queue()
        self.finished = False

        self.status_var = tk.StringVar(value="Preparing installer...")
        self.progress_var = tk.DoubleVar(value=0.0)

        outer = ttk.Frame(self.root, padding=16)
        outer.pack(fill="both", expand=True)

        ttk.Label(outer, text="Recreate the current n8n setup on a new machine").pack(anchor="w")
        ttk.Label(
            outer,
            text="This installer is idempotent: already installed pieces are detected and skipped.",
            foreground="#555555",
        ).pack(anchor="w", pady=(2, 12))

        status_frame = ttk.Frame(outer)
        status_frame.pack(fill="x", pady=(0, 8))
        ttk.Label(status_frame, textvariable=self.status_var).pack(side="left")

        self.progress_bar = ttk.Progressbar(
            outer,
            orient="horizontal",
            mode="indeterminate",
            variable=self.progress_var,
            maximum=100,
        )
        self.progress_bar.pack(fill="x", pady=(0, 12))
        self.progress_bar.start(10)

        self.log_text = tk.Text(
            outer,
            wrap="word",
            height=24,
            state="disabled",
            background="#111111",
            foreground="#F2F2F2",
            insertbackground="#F2F2F2",
        )
        self.log_text.pack(fill="both", expand=True)

        controls = ttk.Frame(outer)
        controls.pack(fill="x", pady=(12, 0))
        ttk.Label(controls, text=f"Detailed log: {LOG_PATH}").pack(side="left")

        self.close_button = ttk.Button(controls, text="Close", command=self.root.destroy, state="disabled")
        self.close_button.pack(side="right")

        self.worker = threading.Thread(target=self.worker_main, daemon=True)

    def start(self) -> None:
        self.root.after(100, self.worker.start)
        self.root.after(100, self.flush_log_queue)
        self.root.mainloop()

    def on_close(self) -> None:
        if self.finished:
            self.root.destroy()
            return
        if messagebox.askyesno(
            APP_TITLE,
            "The installer is still running. Close the window anyway?",
        ):
            self.root.destroy()

    def worker_main(self) -> None:
        try:
            atomic_write_text(LOG_PATH, "")
            self.post_status("Running installer...")
            installer = Installer(self.log)
            installer.run()
            self.post_status("Completed successfully.")
            self.post_complete(success=True)
        except Exception as exc:
            self.log("")
            self.log("Installation failed.")
            self.log(str(exc))
            self.log("")
            self.log(traceback.format_exc().rstrip())
            self.post_status("Failed.")
            self.post_complete(success=False)

    def post_status(self, message: str) -> None:
        self.log_queue.put(("status", message))

    def post_complete(self, *, success: bool) -> None:
        self.log_queue.put(("complete", "success" if success else "failure"))

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        line = f"[{timestamp}] {message}"
        self.log_queue.put(("log", line))
        with LOG_PATH.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(line + "\n")

    def flush_log_queue(self) -> None:
        try:
            while True:
                kind, payload = self.log_queue.get_nowait()
                if kind == "status":
                    self.status_var.set(payload)
                elif kind == "log":
                    self.log_text.configure(state="normal")
                    self.log_text.insert("end", payload + "\n")
                    self.log_text.see("end")
                    self.log_text.configure(state="disabled")
                    match = re.search(r"\[(\d+)/(\d+)\]\s", payload)
                    if match:
                        current_step = int(match.group(1))
                        total_steps = int(match.group(2))
                        self.progress_bar.stop()
                        self.progress_bar.configure(mode="determinate")
                        self.progress_var.set(((current_step - 1) / total_steps) * 100)
                        message = payload.split("] ", 1)[-1]
                        self.status_var.set(message)
                elif kind == "complete":
                    self.finished = True
                    self.progress_bar.stop()
                    self.progress_bar.configure(mode="determinate")
                    self.progress_var.set(100 if payload == "success" else 0)
                    self.close_button.configure(state="normal")
                    if payload == "success":
                        messagebox.showinfo(APP_TITLE, "Installation completed successfully.")
                    else:
                        messagebox.showerror(
                            APP_TITLE,
                            f"Installation failed.\n\nSee log:\n{LOG_PATH}",
                        )
        except queue.Empty:
            pass

        if self.root.winfo_exists():
            self.root.after(100, self.flush_log_queue)


def main() -> None:
    InstallerApp().start()


if __name__ == "__main__":
    main()
