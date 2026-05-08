from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from prefect.logging import get_run_logger


def run_python_module(
    module_name: str,
    arguments: list[str] | None = None,
    working_dir: Path | None = None,
    environment: dict[str, str] | None = None,
) -> None:
    logger = get_run_logger()
    command = [sys.executable, "-m", module_name]
    if arguments:
        command.extend(arguments)

    merged_env = os.environ.copy()
    if environment:
        merged_env.update({key: value for key, value in environment.items() if value is not None})

    logger.info("Ejecutando modulo: {}", " ".join(command))
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
        raise RuntimeError(f"El modulo {module_name} fallo con codigo {return_code}.")
