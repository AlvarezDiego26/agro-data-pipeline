from __future__ import annotations

from pathlib import Path
import tempfile
import zipfile

import polars as pl
from dbfread import DBF

SUPPORTED_MEMBER_EXTENSIONS = {'.dbf'}


def read_dbf(path: Path) -> pl.DataFrame:
    table = DBF(path, load=True, encoding='latin1')
    return pl.DataFrame(iter(table))


def list_zip_members(path: Path) -> list[str]:
    with zipfile.ZipFile(path, 'r') as zf:
        return zf.namelist()


def extract_supported_zip_members(path: Path) -> tuple[Path, list[Path]]:
    temp_dir = Path(tempfile.mkdtemp(prefix='sunat_zip_'))
    extracted: list[Path] = []
    with zipfile.ZipFile(path, 'r') as zf:
        for index, member in enumerate(zf.namelist(), start=1):
            member_path = Path(member)
            if member_path.suffix.lower() not in SUPPORTED_MEMBER_EXTENSIONS:
                continue
            output_path = temp_dir / f'{index:03d}_{member_path.name}'
            with zf.open(member) as src, output_path.open('wb') as dst:
                dst.write(src.read())
            extracted.append(output_path)
    return temp_dir, extracted


def read_supported_file(path: Path) -> pl.DataFrame:
    suffix = path.suffix.lower()
    if suffix == '.dbf':
        return read_dbf(path)
    raise ValueError(f'Extension no soportada para el flujo final SUNAT: {suffix}')
