from sunat_file.config import get_settings
from sunat_file.schemas import SunatFile


def scan_inbox() -> list[SunatFile]:
    settings = get_settings()
    files: list[SunatFile] = []
    for path in settings.sunat_inbox_dir.glob('*'):
        if path.is_file():
            files.append(SunatFile(path=path, extension=path.suffix.lower(), source_name=path.name))
    return sorted(files, key=lambda item: item.source_name)
