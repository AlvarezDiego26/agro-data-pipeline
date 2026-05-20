import hashlib
import re
from pathlib import Path
from urllib.parse import urljoin, unquote
import httpx
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import BaseModel

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


class MidagriMonthlyRemoteFile(BaseModel):
    file_name: str
    url: str
    extension: str
    source_page_url: str
    title: str
    publication_year: int | None
    content_length: int | None
    last_modified: str | None
    remote_signature: str


def _build_remote_signature(
    *,
    file_name: str,
    url: str,
    content_length: int | None,
    last_modified: str | None,
) -> str:
    payload = "|".join(
        [
            file_name.strip().upper(),
            url.strip(),
            str(content_length or ""),
            (last_modified or "").strip(),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _sanitize_file_name(value: str) -> str:
    """Limpia caracteres inválidos de Windows para nombres de archivos."""
    cleaned = re.sub(r'[<>:"/\\|?*]', "_", value).strip().rstrip(".")
    return cleaned or "archivo_mensual"


def _http_head_metadata(client: httpx.Client, url: str) -> tuple[int | None, str | None]:
    """Obtiene Content-Length y Last-Modified mediante un request HEAD."""
    try:
        response = client.head(url, headers={"User-Agent": USER_AGENT})
        if response.status_code == 200:
            raw_length = response.headers.get("Content-Length", "").strip()
            content_length = int(raw_length) if raw_length.isdigit() else None
            last_modified = response.headers.get("Last-Modified")
            return content_length, (last_modified or "").strip() or None
    except Exception:
        pass
    return None, None


def fetch_monthly_remote_listing() -> list[MidagriMonthlyRemoteFile]:
    """
    Crawlea el portal del MIDAGRI (Moderno e Histórico) para descubrir los boletines mensuales de 'El Agro en Cifras'.
    Retorna una lista de metadatos de archivos remotos descubiertos (ZIP y Excel).
    """
    logger.info("Iniciando escaneo de boletines mensuales remotos de El Agro en Cifras...")
    discovered: dict[str, MidagriMonthlyRemoteFile] = {}
    
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3"
    }

    with httpx.Client(verify=False, follow_redirects=True, timeout=30) as client:
        # ==========================================
        # 1. Portal Moderno (2019 - 2026)
        # ==========================================
        collection_url = "https://www.gob.pe/institucion/midagri/colecciones/388-boletin-estadistico-mensual-el-agro-en-cifras"
        try:
            logger.info(f"Crawleando colección Portal Moderno: {collection_url}")
            response = client.get(collection_url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "lxml")
                
                # Buscar enlaces de informes anuales (ej: 2026, 2025, etc.)
                annual_links = []
                for link in soup.find_all("a", href=True):
                    href = link["href"].strip()
                    if "boletin-estadistico-mensual-el-agro-en-cifras-" in href:
                        full_url = urljoin(collection_url, href)
                        if full_url not in annual_links:
                            annual_links.append(full_url)
                
                logger.info(f"Páginas anuales descubiertas en Portal Moderno: {len(annual_links)}")
                
                # Para cada página anual, extraer los ZIPs/Excel del reporte
                for annual_page in annual_links:
                    logger.debug(f"Escaneando página anual: {annual_page}")
                    ann_resp = client.get(annual_page, headers=headers)
                    if ann_resp.status_code != 200:
                        continue
                    
                    ann_soup = BeautifulSoup(ann_resp.text, "lxml")
                    
                    # Extraer año de la página
                    year_match = re.search(r"cifras-(\d{4})", annual_page)
                    pub_year = int(year_match.group(1)) if year_match else None
                    
                    # Buscar descargas de archivos ZIP o Excel
                    for link in ann_soup.find_all("a", href=True):
                        href = link["href"].strip()
                        # Las URLs de descarga física suelen contener '/uploads/document/file/'
                        # O contener extensiones de ZIP/Excel
                        is_download = "/uploads/document/file/" in href or ".zip" in href.lower() or ".xlsx" in href.lower() or ".xls" in href.lower()
                        if is_download:
                            full_download_url = urljoin(annual_page, href)
                            
                            # Limpiar nombre del archivo
                            raw_name = unquote(Path(full_download_url.split("?", 1)[0]).name)
                            sanitized_name = _sanitize_file_name(raw_name)
                            
                            # Filtrar solo si es ZIP o Excel
                            ext = Path(sanitized_name).suffix.lower()
                            if ext not in {".zip", ".xlsx", ".xls"}:
                                continue
                                
                            # Consultar metadatos HEAD para obtener firma remota única
                            content_length, last_modified = _http_head_metadata(client, full_download_url)
                            
                            key = sanitized_name.upper()
                            if key not in discovered:
                                discovered[key] = MidagriMonthlyRemoteFile(
                                    file_name=sanitized_name,
                                    url=full_download_url,
                                    extension=ext,
                                    source_page_url=annual_page,
                                    title=sanitized_name,
                                    publication_year=pub_year,
                                    content_length=content_length,
                                    last_modified=last_modified,
                                    remote_signature=_build_remote_signature(
                                        file_name=sanitized_name,
                                        url=full_download_url,
                                        content_length=content_length,
                                        last_modified=last_modified,
                                    )
                                )
            else:
                logger.warning(f"Error cargando colección moderno (Status: {response.status_code})")
        except Exception as e:
            logger.error(f"Error procesando Portal Moderno: {e}")

        # ==========================================
        # 2. Portal Histórico (2016 - 2018)
        # ==========================================
        historical_url = "https://www.midagri.gob.pe/portal/boletin-estadistico-mensual-el-agro-en-cifras"
        try:
            logger.info(f"Crawleando Portal Histórico: {historical_url}")
            
            # Paginar start=1 (2018), start=2 (2017), start=3 (2016)
            for page_idx in [1, 2, 3]:
                url = f"{historical_url}?start={page_idx}"
                logger.debug(f"Paginando portal histórico mensual: {url}")
                hist_resp = client.get(url, headers=headers)
                if hist_resp.status_code != 200:
                    continue
                    
                hist_soup = BeautifulSoup(hist_resp.text, "lxml")
                
                # El año se deduce del índice
                # start=1 -> 2018, start=2 -> 2017, start=3 -> 2016
                pub_year = 2019 - page_idx
                
                for link in hist_soup.find_all("a", href=True):
                    href = link["href"].strip()
                    # Buscar enlaces con descargas de ZIP o Excel directo
                    is_download = "?download=" in href or ".zip" in href.lower() or ".xlsx" in href.lower() or ".xls" in href.lower()
                    if is_download:
                        full_download_url = urljoin(url, href)
                        
                        # Decodificar nombre
                        from urllib.parse import urlparse, parse_qs
                        parsed_url = urlparse(full_download_url)
                        download_params = parse_qs(parsed_url.query).get("download")
                        
                        raw_name = ""
                        if download_params:
                            download_val = download_params[0]
                            if ":" in download_val:
                                raw_name = download_val.split(":", 1)[1] + ".zip"
                            else:
                                raw_name = download_val + ".zip"
                        else:
                            raw_name = Path(full_download_url.split("?", 1)[0]).name
                        
                        # Si no contiene extensión válida, agregar .zip por defecto para descargas joomla
                        sanitized_name = _sanitize_file_name(unquote(raw_name))
                        ext = Path(sanitized_name).suffix.lower()
                        if ext not in {".zip", ".xlsx", ".xls"}:
                            sanitized_name += ".zip"
                            ext = ".zip"
                            
                        content_length, last_modified = _http_head_metadata(client, full_download_url)
                        
                        key = sanitized_name.upper()
                        if key not in discovered:
                            discovered[key] = MidagriMonthlyRemoteFile(
                                file_name=sanitized_name,
                                url=full_download_url,
                                extension=ext,
                                source_page_url=url,
                                title=sanitized_name,
                                publication_year=pub_year,
                                content_length=content_length,
                                last_modified=last_modified,
                                remote_signature=_build_remote_signature(
                                    file_name=sanitized_name,
                                    url=full_download_url,
                                    content_length=content_length,
                                    last_modified=last_modified,
                                )
                            )
        except Exception as e:
            logger.error(f"Error procesando Portal Histórico: {e}")

    # Retornar ordenados por año de publicación y nombre de archivo
    logger.info(f"Total de boletines mensuales remotos descubiertos: {len(discovered)}")
    return sorted(discovered.values(), key=lambda item: ((item.publication_year or 0), item.file_name))


def download_monthly_file(file: MidagriMonthlyRemoteFile, timeout: int = 30) -> bytes:
    """Descarga los bytes de un archivo mensual remoto en memoria."""
    logger.info(f"Descargando archivo mensual remoto: {file.file_name} desde {file.url}")
    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(verify=False, follow_redirects=True, timeout=timeout) as client:
        response = client.get(file.url, headers=headers)
        if response.status_code != 200:
            raise httpx.HTTPStatusError(
                f"Fallo de descarga con status code {response.status_code}",
                request=response.request,
                response=response
            )
        logger.success(f"Descarga exitosa de {file.file_name} ({len(response.content)} bytes)")
        return response.content
