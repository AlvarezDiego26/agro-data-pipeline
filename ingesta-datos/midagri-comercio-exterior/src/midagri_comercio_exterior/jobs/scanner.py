from midagri_comercio_exterior.config import get_settings
from midagri_comercio_exterior.schemas import MidagriFile


def scan_inbox() -> list[MidagriFile]:
    settings = get_settings()
    files: list[MidagriFile] = []
    for path in settings.midagri_ce_inbox_dir.glob("*"):
        if path.is_file():
            files.append(
                MidagriFile(path=path, extension=path.suffix.lower(), source_name=path.name)
            )
    return sorted(files, key=lambda item: item.source_name)
