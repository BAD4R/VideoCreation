#!/usr/bin/env python3
"""
Import and publish n8n workflows from a folder with auto-detection of n8n installation mode.

Supported modes:
- local n8n binary in PATH (or APPDATA\\npm\\n8n.cmd on Windows)
- npx --no-install n8n
- docker exec <container> n8n
"""

import argparse
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple


ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass
class Runner:
    mode: str
    command_prefix: List[str]
    version: str
    source: str
    docker_bin: Optional[str] = None
    container_id: Optional[str] = None
    container_name: Optional[str] = None


def format_command(cmd: Sequence[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(list(cmd))
    return shlex.join(list(cmd))


def run_command(
    cmd: Sequence[str],
    *,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        list(cmd),
        text=True,
        capture_output=capture_output,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            "Command failed ({code}): {cmd}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}".format(
                code=result.returncode,
                cmd=format_command(cmd),
                stdout=result.stdout or "",
                stderr=result.stderr or "",
            )
        )
    return result


def first_non_empty_line(text: str) -> str:
    for line in text.splitlines():
        trimmed = line.strip()
        if trimmed:
            return trimmed
    return "unknown"


def probe_n8n_version(prefix: Sequence[str]) -> Optional[str]:
    result = run_command([*prefix, "--version"], check=False)
    if result.returncode != 0:
        return None
    combined = (result.stdout or "") + "\n" + (result.stderr or "")
    return first_non_empty_line(combined)


def detect_local_runner() -> Optional[Runner]:
    candidates: List[str] = []
    n8n_path = shutil.which("n8n")
    if n8n_path:
        candidates.append(n8n_path)

    if os.name == "nt":
        appdata = os.environ.get("APPDATA")
        if appdata:
            win_cmd = Path(appdata) / "npm" / "n8n.cmd"
            if win_cmd.exists():
                candidates.append(str(win_cmd))

    seen = set()
    for candidate in candidates:
        normalized = os.path.normcase(os.path.abspath(candidate))
        if normalized in seen:
            continue
        seen.add(normalized)
        version = probe_n8n_version([candidate])
        if version:
            return Runner(
                mode="local",
                command_prefix=[candidate],
                version=version,
                source=candidate,
            )
    return None


def detect_npx_runner(allow_install: bool) -> Optional[Runner]:
    npx_path = shutil.which("npx")
    if not npx_path:
        return None

    no_install_prefix = [npx_path, "--no-install", "n8n"]
    version = probe_n8n_version(no_install_prefix)
    if version:
        return Runner(
            mode="npx",
            command_prefix=no_install_prefix,
            version=version,
            source="npx --no-install n8n",
        )

    if not allow_install:
        return None

    install_prefix = [npx_path, "n8n"]
    version = probe_n8n_version(install_prefix)
    if version:
        return Runner(
            mode="npx",
            command_prefix=install_prefix,
            version=version,
            source="npx n8n",
        )
    return None


def list_running_containers(docker_bin: str) -> List[Tuple[str, str, str]]:
    result = run_command(
        [docker_bin, "ps", "--format", "{{.ID}}\t{{.Image}}\t{{.Names}}"],
        check=False,
    )
    if result.returncode != 0:
        return []

    containers = []
    for line in (result.stdout or "").splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        cid = parts[0].strip()
        image = parts[1].strip()
        name = parts[2].strip()
        if cid:
            containers.append((cid, image, name))
    return containers


def detect_docker_runners(
    *,
    preferred_container: Optional[str],
) -> List[Runner]:
    docker_bin = shutil.which("docker")
    if not docker_bin:
        return []

    info = run_command([docker_bin, "info"], check=False)
    if info.returncode != 0:
        return []

    containers = list_running_containers(docker_bin)
    if not containers:
        return []

    selected: List[Tuple[str, str, str]] = []
    if preferred_container:
        target = preferred_container.strip()
        for cid, image, name in containers:
            if cid == target or cid.startswith(target) or name == target:
                selected.append((cid, image, name))
        if not selected:
            raise RuntimeError(
                "Docker container '{}' is not running.".format(preferred_container)
            )
    else:
        for cid, image, name in containers:
            haystack = "{} {}".format(image, name).lower()
            if "n8n" in haystack:
                selected.append((cid, image, name))

    runners: List[Runner] = []
    for cid, image, name in selected:
        version = probe_n8n_version([docker_bin, "exec", cid, "n8n"])
        if version:
            runners.append(
                Runner(
                    mode="docker",
                    command_prefix=[docker_bin, "exec", "-i", cid, "n8n"],
                    version=version,
                    source="docker exec {} n8n ({})".format(cid, image),
                    docker_bin=docker_bin,
                    container_id=cid,
                    container_name=name,
                )
            )
    return runners


def pick_runner(
    *,
    mode: str,
    local_runner: Optional[Runner],
    npx_runner: Optional[Runner],
    docker_runners: List[Runner],
) -> Runner:
    if mode == "local":
        if not local_runner:
            raise RuntimeError("Local n8n executable is not available.")
        return local_runner

    if mode == "npx":
        if not npx_runner:
            raise RuntimeError("npx n8n is not available.")
        return npx_runner

    if mode == "docker":
        if not docker_runners:
            raise RuntimeError("No compatible running Docker container with n8n found.")
        if len(docker_runners) > 1:
            options = ", ".join(
                "{} ({})".format(r.container_name or r.container_id, r.container_id)
                for r in docker_runners
            )
            raise RuntimeError(
                "Multiple Docker n8n containers detected: {}. Use --docker-container."
                .format(options)
            )
        return docker_runners[0]

    candidates: List[Runner] = []
    if local_runner:
        candidates.append(local_runner)
    if docker_runners:
        if len(docker_runners) == 1:
            candidates.append(docker_runners[0])
        else:
            options = ", ".join(
                "{} ({})".format(r.container_name or r.container_id, r.container_id)
                for r in docker_runners
            )
            raise RuntimeError(
                "Multiple Docker n8n containers detected: {}. "
                "Use --mode docker --docker-container <name|id>."
                .format(options)
            )
    # Treat npx as a fallback when no direct local binary was detected.
    if npx_runner and not local_runner:
        candidates.append(npx_runner)

    if not candidates:
        raise RuntimeError(
            "Could not detect n8n. Checked local binary, npx, and Docker."
        )

    if len(candidates) > 1:
        options = ", ".join("{} [{}]".format(c.source, c.mode) for c in candidates)
        raise RuntimeError(
            "Ambiguous n8n installation detected: {}. "
            "Use --mode (local|docker|npx) to select target."
            .format(options)
        )

    return candidates[0]


def ensure_workflows_dir(path_value: str) -> Path:
    path = Path(path_value).expanduser().resolve()
    if not path.exists():
        raise RuntimeError("Workflows directory does not exist: {}".format(path))
    if not path.is_dir():
        raise RuntimeError("Workflows path is not a directory: {}".format(path))

    json_files = list(path.glob("*.json"))
    if not json_files:
        raise RuntimeError(
            "No *.json files found in workflows directory: {}".format(path)
        )
    return path


def prepare_docker_input(
    runner: Runner,
    workflows_dir: Path,
) -> str:
    assert runner.docker_bin and runner.container_id
    stamp = int(time.time())
    parent = "/tmp/codex-n8n-import-{}".format(stamp)
    run_command([runner.docker_bin, "exec", runner.container_id, "mkdir", "-p", parent])
    run_command(
        [
            runner.docker_bin,
            "cp",
            str(workflows_dir),
            "{}:{}".format(runner.container_id, parent),
        ]
    )
    return "{}/{}".format(parent, workflows_dir.name)


def extract_workflow_ids(text: str) -> List[str]:
    found: List[str] = []
    seen = set()

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        candidates = [line]
        if " " in line:
            candidates.append(line.split(" ", 1)[0].strip())

        for candidate in candidates:
            if ID_PATTERN.fullmatch(candidate) and candidate not in seen:
                seen.add(candidate)
                found.append(candidate)

    return found


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import and publish n8n workflows from a local folder."
    )
    parser.add_argument(
        "--workflows-dir",
        default="N8N_WORKFLOWS",
        help="Path to folder with exported workflow JSON files (default: ./N8N_WORKFLOWS).",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "local", "docker", "npx"],
        default="auto",
        help="How to run n8n CLI (default: auto).",
    )
    parser.add_argument(
        "--docker-container",
        default=None,
        help="Docker container name or ID (used when mode=docker or to disambiguate).",
    )
    parser.add_argument(
        "--allow-npx-install",
        action="store_true",
        help="Allow npx to download n8n if not already installed.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect and print planned commands without changing workflows.",
    )
    parser.add_argument(
        "--skip-import",
        action="store_true",
        help="Skip import step.",
    )
    parser.add_argument(
        "--skip-publish",
        action="store_true",
        help="Skip publish step.",
    )
    parser.add_argument(
        "--continue-on-publish-error",
        action="store_true",
        help="Continue publishing remaining workflows if one fails.",
    )
    args = parser.parse_args()

    workflows_dir = ensure_workflows_dir(args.workflows_dir)
    local_runner = detect_local_runner()
    npx_runner = detect_npx_runner(args.allow_npx_install)
    docker_runners = detect_docker_runners(preferred_container=args.docker_container)
    runner = pick_runner(
        mode=args.mode,
        local_runner=local_runner,
        npx_runner=npx_runner,
        docker_runners=docker_runners,
    )

    print("Detected n8n runner: {} (mode={}, version={})".format(
        runner.source, runner.mode, runner.version
    ))
    print("Workflows directory: {}".format(workflows_dir))

    import_input_path = str(workflows_dir)
    if runner.mode == "docker":
        import_input_path = prepare_docker_input(runner, workflows_dir)
        print("Copied workflows into container path: {}".format(import_input_path))

    import_cmd = [*runner.command_prefix, "import:workflow", "--separate", "--input", import_input_path]
    list_cmd = [*runner.command_prefix, "list:workflow", "--onlyId"]

    if args.dry_run:
        if not args.skip_import:
            print("[dry-run] Import command: {}".format(format_command(import_cmd)))
        if not args.skip_publish:
            print("[dry-run] List command:   {}".format(format_command(list_cmd)))
            print("[dry-run] Publish each ID returned by list:workflow --onlyId")
        return 0

    if not args.skip_import:
        print("Importing workflows...")
        run_command(import_cmd)
        print("Import completed.")

    if args.skip_publish:
        return 0

    print("Reading workflow IDs...")
    list_result = run_command(list_cmd)
    workflow_ids = extract_workflow_ids(list_result.stdout or "")
    if not workflow_ids:
        raise RuntimeError("No workflow IDs found from list:workflow --onlyId output.")

    print("Publishing {} workflows...".format(len(workflow_ids)))
    failed: List[Tuple[str, str]] = []
    for workflow_id in workflow_ids:
        publish_cmd = [*runner.command_prefix, "publish:workflow", "--id", workflow_id]
        result = run_command(publish_cmd, check=False)
        if result.returncode == 0:
            print("Published workflow {}".format(workflow_id))
            continue

        error_text = (result.stderr or result.stdout or "").strip()
        failed.append((workflow_id, error_text))
        print("Failed to publish {}: {}".format(workflow_id, error_text))
        if not args.continue_on_publish_error:
            raise RuntimeError(
                "Publishing stopped at workflow {}. Re-run with --continue-on-publish-error to skip failures."
                .format(workflow_id)
            )

    if failed:
        print("Done with errors. Failed: {}".format(len(failed)))
        return 2

    print("Done. All workflows published successfully.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted.")
        raise SystemExit(130)
    except RuntimeError as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        raise SystemExit(1)
