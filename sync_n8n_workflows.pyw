#!/usr/bin/env python3
"""
Pull the git repo, import only updated n8n workflows, then publish only those workflows.

Supported modes:
- local n8n binary in PATH (or APPDATA\\npm\\n8n.cmd on Windows)
- npx --no-install n8n
- docker exec <container> n8n
"""

import argparse
import ctypes
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_WORKFLOWS_DIR = SCRIPT_DIR / "N8N_WORKFLOWS"


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
    cwd: Optional[Path] = None,
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        list(cmd),
        text=True,
        capture_output=capture_output,
        cwd=str(cwd) if cwd else None,
    )
    if check and result.returncode != 0:
        if capture_output:
            raise RuntimeError(
                "Command failed ({code}): {cmd}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}".format(
                    code=result.returncode,
                    cmd=format_command(cmd),
                    stdout=result.stdout or "",
                    stderr=result.stderr or "",
                )
            )
        raise RuntimeError(
            "Command failed ({code}): {cmd}. See console output above.".format(
                code=result.returncode,
                cmd=format_command(cmd),
            )
        )
    return result


def allocate_console_if_needed() -> bool:
    if os.name != "nt":
        return False

    stdout = getattr(sys, "stdout", None)
    if stdout is not None:
        try:
            if stdout.isatty():
                return False
        except Exception:
            pass

    kernel32 = ctypes.windll.kernel32
    if kernel32.GetConsoleWindow():
        return False

    if not kernel32.AllocConsole():
        return False

    sys.stdin = open("CONIN$", "r", encoding="utf-8", buffering=1)
    console_out = open("CONOUT$", "w", encoding="utf-8", buffering=1)
    sys.stdout = console_out
    sys.stderr = console_out
    kernel32.SetConsoleTitleW("n8n workflow sync")
    return True


def pause_before_exit(console_allocated: bool, exit_code: int) -> None:
    if not console_allocated:
        return

    print()
    prompt = (
        "Finished. Press Enter to close..."
        if exit_code == 0
        else "Finished with errors. Press Enter to close..."
    )
    try:
        input(prompt)
    except EOFError:
        time.sleep(5)


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


def run_git_command(
    repo_dir: Path,
    args: Sequence[str],
    *,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess:
    return run_command(
        ["git", *args],
        check=check,
        capture_output=capture_output,
        cwd=repo_dir,
    )


def ensure_git_repository(path: Path) -> Path:
    result = run_git_command(path, ["rev-parse", "--show-toplevel"])
    return Path((result.stdout or "").strip()).resolve()


def ensure_workflows_dir(path_value: str) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = (SCRIPT_DIR / path).resolve()
    else:
        path = path.resolve()

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


def get_head_commit(repo_dir: Path) -> str:
    result = run_git_command(repo_dir, ["rev-parse", "HEAD"])
    return (result.stdout or "").strip()


def unique_paths(paths: Sequence[Path]) -> List[Path]:
    unique: List[Path] = []
    seen = set()
    for path in paths:
        normalized = os.path.normcase(str(path.resolve()))
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(path)
    return unique


def collect_changed_workflow_files(
    repo_dir: Path,
    workflows_dir: Path,
    before_head: str,
    after_head: str,
) -> List[Path]:
    if before_head == after_head:
        return []

    try:
        workflows_rel = workflows_dir.relative_to(repo_dir)
    except ValueError as exc:
        raise RuntimeError(
            "Workflows directory must be inside the git repository: {}".format(
                workflows_dir
            )
        ) from exc

    diff_result = run_git_command(
        repo_dir,
        [
            "diff",
            "--name-only",
            "--diff-filter=ACMRT",
            before_head,
            after_head,
            "--",
            workflows_rel.as_posix(),
        ],
    )

    changed_files: List[Path] = []
    for raw_path in (diff_result.stdout or "").splitlines():
        rel_path = raw_path.strip()
        if not rel_path:
            continue
        abs_path = (repo_dir / Path(rel_path)).resolve()
        if abs_path.suffix.lower() != ".json":
            continue
        if abs_path.exists():
            changed_files.append(abs_path)

    return unique_paths(changed_files)


def load_workflow_ids(workflow_files: Sequence[Path]) -> List[str]:
    workflow_ids: List[str] = []
    seen = set()

    for workflow_file in workflow_files:
        with workflow_file.open("r", encoding="utf-8") as handle:
            payload: Any = json.load(handle)

        entries: List[Any]
        if isinstance(payload, list):
            entries = payload
        else:
            entries = [payload]

        file_ids: List[str] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            workflow_id = entry.get("id")
            if workflow_id is None:
                continue
            workflow_id = str(workflow_id).strip()
            if workflow_id:
                file_ids.append(workflow_id)

        if not file_ids:
            raise RuntimeError(
                "Could not find workflow id in file: {}".format(workflow_file)
            )

        for workflow_id in file_ids:
            if workflow_id in seen:
                continue
            seen.add(workflow_id)
            workflow_ids.append(workflow_id)

    return workflow_ids


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


def print_workflow_summary(
    workflow_files: Sequence[Path],
    workflow_ids: Sequence[str],
) -> None:
    print("Changed workflows: {}".format(len(workflow_files)))
    for workflow_file, workflow_id in zip(workflow_files, workflow_ids):
        print(" - {} (id={})".format(workflow_file.name, workflow_id))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pull git, then import and publish only updated n8n workflows."
    )
    parser.add_argument(
        "--workflows-dir",
        default=str(DEFAULT_WORKFLOWS_DIR),
        help="Path to folder with exported workflow JSON files (default: script_dir/N8N_WORKFLOWS).",
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
        help="Print planned actions without running git pull or touching workflows.",
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

    repo_dir = ensure_git_repository(SCRIPT_DIR)
    workflows_dir = ensure_workflows_dir(args.workflows_dir)

    print("Script directory: {}".format(SCRIPT_DIR))
    print("Git repository: {}".format(repo_dir))
    print("Workflows directory: {}".format(workflows_dir))

    if args.dry_run:
        print("[dry-run] Git command: {}".format(format_command(["git", "pull"])))
        print("[dry-run] Changed workflow files are detected from the git diff after pull.")
        changed_workflow_files: List[Path] = []
        workflow_ids: List[str] = []
    else:
        before_head = get_head_commit(repo_dir)
        print("Running git pull...")
        run_git_command(repo_dir, ["pull"], capture_output=False)
        after_head = get_head_commit(repo_dir)

        changed_workflow_files = collect_changed_workflow_files(
            repo_dir,
            workflows_dir,
            before_head,
            after_head,
        )

        if before_head == after_head:
            print("Git is already up to date. Nothing to import or publish.")
            return 0

        if not changed_workflow_files:
            print(
                "Git updated, but no workflow JSON files changed in {}.".format(
                    workflows_dir
                )
            )
            return 0

        workflow_ids = load_workflow_ids(changed_workflow_files)
        print_workflow_summary(changed_workflow_files, workflow_ids)

    local_runner = detect_local_runner()
    npx_runner = detect_npx_runner(args.allow_npx_install)
    docker_runners = detect_docker_runners(preferred_container=args.docker_container)
    runner = pick_runner(
        mode=args.mode,
        local_runner=local_runner,
        npx_runner=npx_runner,
        docker_runners=docker_runners,
    )

    print(
        "Detected n8n runner: {} (mode={}, version={})".format(
            runner.source, runner.mode, runner.version
        )
    )

    if args.dry_run:
        if not args.skip_import:
            print("[dry-run] Import only the workflow files changed by git pull.")
        if not args.skip_publish:
            print(
                "[dry-run] Publish only the workflow IDs extracted from those changed files."
            )
        return 0

    if args.skip_import and args.skip_publish:
        print("Import and publish are both skipped. Nothing else to do.")
        return 0

    with tempfile.TemporaryDirectory(prefix="n8n-workflow-sync-") as bundle_root:
        bundle_dir = Path(bundle_root)
        for workflow_file in changed_workflow_files:
            shutil.copy2(workflow_file, bundle_dir / workflow_file.name)

        import_input_path = str(bundle_dir)
        if runner.mode == "docker":
            import_input_path = prepare_docker_input(runner, bundle_dir)
            print(
                "Copied changed workflows into container path: {}".format(
                    import_input_path
                )
            )

        import_cmd = [
            *runner.command_prefix,
            "import:workflow",
            "--separate",
            "--input",
            import_input_path,
        ]

        if not args.skip_import:
            print("Importing {} changed workflows...".format(len(changed_workflow_files)))
            run_command(import_cmd, capture_output=False)
            print("Import completed.")

    if args.skip_publish:
        print("Publish step skipped.")
        return 0

    print("Publishing {} changed workflows...".format(len(workflow_ids)))
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

    print("Done. Changed workflows imported and published successfully.")
    return 0


if __name__ == "__main__":
    console_allocated = allocate_console_if_needed()
    exit_code = 0
    try:
        exit_code = main()
    except KeyboardInterrupt:
        print("Interrupted.")
        exit_code = 130
    except RuntimeError as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        exit_code = 1
    except Exception as exc:
        print("UNEXPECTED ERROR: {}".format(exc), file=sys.stderr)
        exit_code = 1
    finally:
        pause_before_exit(console_allocated, exit_code)

    raise SystemExit(exit_code)
