from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from prefect.logging import get_run_logger


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


def install_requirements(
    requirements_file: Path,
    working_dir: Path,
    environment: dict[str, str] | None = None,
) -> None:
    run_command(
        [sys.executable, "-m", "pip", "install", "-r", str(requirements_file)],
        working_dir=working_dir,
        environment=environment,
    )


def run_python_module(
    module_name: str,
    arguments: list[str] | None = None,
    working_dir: Path | None = None,
    environment: dict[str, str] | None = None,
) -> None:
    command = [sys.executable, "-m", module_name]
    if arguments:
        command.extend(arguments)
    run_command(command, working_dir=working_dir, environment=environment)

