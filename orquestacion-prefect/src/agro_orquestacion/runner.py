from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path

from prefect.logging import get_run_logger

RUNTIME_WAIT_SECONDS = 600


def run_command(
    command: list[str],
    working_dir: Path | None = None,
    environment: dict[str, str] | None = None,
) -> None:
    logger = get_run_logger()
    merged_env = os.environ.copy()
    if environment:
        merged_env.update({key: value for key, value in environment.items() if value is not None})

    logger.info("Ejecutando comando: %s", " ".join(command))
    process = subprocess.Popen(
        command,
        cwd=str(working_dir or Path.cwd()),
        env=merged_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert process.stdout is not None
    for line in process.stdout:
        text = line.rstrip()
        if text:
            logger.info(text)

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"El comando {' '.join(command)} fallo con codigo {return_code}.")


def _python_executable(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _requirements_signature(requirements_path: Path) -> str:
    return hashlib.sha256(requirements_path.read_bytes()).hexdigest()


def ensure_runtime_python(
    runtime_name: str,
    requirements_path: Path,
    working_dir: Path,
    venvs_root: Path,
) -> Path:
    logger = get_run_logger()
    venv_dir = venvs_root / runtime_name
    signature_path = venv_dir / ".requirements.sha256"
    ready_path = venv_dir / ".ready"
    lock_path = venv_dir / ".bootstrap.lock"
    expected_signature = _requirements_signature(requirements_path)
    python_path = _python_executable(venv_dir)

    venv_dir.mkdir(parents=True, exist_ok=True)

    def runtime_ready() -> bool:
        return (
            python_path.exists()
            and ready_path.exists()
            and signature_path.exists()
            and signature_path.read_text(encoding="utf-8").strip() == expected_signature
        )

    if runtime_ready():
        return python_path

    start_time = time.monotonic()
    lock_owner = False
    while not lock_owner:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
                lock_file.write(str(os.getpid()))
            lock_owner = True
        except FileExistsError:
            if runtime_ready():
                return python_path
            if time.monotonic() - start_time > RUNTIME_WAIT_SECONDS:
                raise TimeoutError(
                    f"Timeout esperando el runtime aislado '{runtime_name}'."
                )
            time.sleep(2)

    try:
        if runtime_ready():
            return python_path

        logger.info("Preparando runtime aislado '%s' en %s", runtime_name, venv_dir)
        if not python_path.exists():
            run_command([sys.executable, "-m", "venv", str(venv_dir)], working_dir=working_dir)

        run_command(
            [str(python_path), "-m", "pip", "install", "-r", str(requirements_path)],
            working_dir=working_dir,
        )
        signature_path.write_text(expected_signature, encoding="utf-8")
        ready_path.write_text("ready\n", encoding="utf-8")
        return python_path
    finally:
        if lock_path.exists():
            lock_path.unlink()


def run_python_module(
    module_name: str,
    arguments: list[str] | None = None,
    working_dir: Path | None = None,
    environment: dict[str, str] | None = None,
    python_executable: str | Path | None = None,
) -> None:
    executable = str(python_executable or sys.executable)
    command = [executable, "-m", module_name]
    if arguments:
        command.extend(arguments)
    run_command(command, working_dir=working_dir, environment=environment)
