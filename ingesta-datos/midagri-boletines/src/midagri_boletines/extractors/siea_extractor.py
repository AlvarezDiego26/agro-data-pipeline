from datetime import date
import re
import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

SPANISH_MONTHS = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "set", 10: "oct", 11: "nov", 12: "dic"
}

SPANISH_MONTH_NAMES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
}

# Caché en memoria para los enlaces de descarga del portal histórico (2016 - 2018)
# Estructura: { año: { fecha: url_descarga } }
_HISTORICAL_LINKS_CACHE: dict[int, dict[date, str]] = {}


def _parse_spanish_date_string(s: str) -> date | None:
    """Parsea una cadena de fecha larga en español (ej: '31-de-diciembre-2018') a date."""
    normalized = s.lower().replace("-", " ").replace(" de ", " ").strip()
    parts = [p for p in normalized.split() if p]
    if len(parts) >= 3:
        day_str = parts[0]
        month_str = parts[1]
        year_str = parts[-1]
        try:
            day = int(day_str)
            year = int(year_str)
            month = SPANISH_MONTH_NAMES.get(month_str)
            if month and 1 <= day <= 31 and 2000 <= year <= 2100:
                return date(year, month, day)
        except ValueError:
            pass
    return None


def _parse_short_date_string(s: str) -> date | None:
    """Parsea una fecha corta del nombre de archivo (ej: 'sisap-ingreso-gmml-31dic18.pdf') a date."""
    match = re.search(r"(\d{1,2})([a-z]{3})(\d{2})", s.lower())
    if match:
        day = int(match.group(1))
        month_str = match.group(2)
        year_val = int(match.group(3))
        
        short_months = {
            "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
            "jul": 7, "ago": 8, "sep": 9, "set": 9, "oct": 10, "nov": 11, "dic": 12
        }
        month = short_months.get(month_str)
        if month:
            year = 2000 + year_val
            return date(year, month, day)
    return None


def _crawl_historical_links_for_year(year: int) -> dict[date, str]:
    """Copia y mapea los enlaces de descarga para un año histórico dado del portal antiguo."""
    logger.info(f"Crawleando enlaces históricos del año {year} desde el portal antiguo...")
    year_links: dict[date, str] = {}
    base_url = f"https://www.midagri.gob.pe/portal/reporte-gran-mercado-mayorista-de-lima/la-parada-{year}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    
    # Paginar hasta que no encontremos nuevos enlaces de descarga
    start = 0
    with httpx.Client(verify=False, follow_redirects=True, timeout=30) as client:
        while start <= 400:
            url = base_url if start == 0 else f"{base_url}?start={start}"
            try:
                logger.debug(f"Paginando portal histórico: {url}")
                response = client.get(url, headers=headers)
                if response.status_code != 200:
                    logger.warning(f"Error cargando portal histórico {url} (Status: {response.status_code})")
                    break
                
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, "lxml")
                
                links = soup.find_all("a", href=True)
                found_on_page = 0
                
                for link in links:
                    href = link["href"].strip()
                    if "?download=" in href:
                        from urllib.parse import urlparse, parse_qs
                        parsed_url = urlparse(href)
                        download_params = parse_qs(parsed_url.query).get("download")
                        if not download_params:
                            continue
                        download_val = download_params[0]
                        
                        parsed_date = None
                        if ":" in download_val:
                            date_part = download_val.split(":", 1)[1]
                            parsed_date = _parse_spanish_date_string(date_part)
                        
                        if not parsed_date:
                            link_text = link.get_text().strip()
                            parsed_date = _parse_short_date_string(link_text)
                        
                        if parsed_date and parsed_date.year == year:
                            full_download_url = httpx.URL(base_url).join(href)
                            if parsed_date not in year_links:
                                year_links[parsed_date] = str(full_download_url)
                                found_on_page += 1
                
                logger.debug(f"Página con start={start}: se encontraron {found_on_page} enlaces nuevos/válidos.")
                if found_on_page == 0:
                    break
                
                start += 20
            except Exception as e:
                logger.error(f"Error crawleando portal histórico {url}: {e}")
                break
                
    logger.info(f"Crawl completado para el año {year}. Enlaces totales encontrados: {len(year_links)}")
    return year_links


def _get_url_candidates(target_date: date) -> list[str]:
    """Genera las posibles URLs de descarga del PDF para una fecha dada (2019-2026)."""
    year = target_date.year
    month_num = f"{target_date.month:02d}"
    day_2 = f"{target_date.day:02d}"
    day_1 = f"{target_date.day}"
    short_month = SPANISH_MONTHS[target_date.month]
    short_year = f"{target_date.year % 100:02d}"

    candidates = []

    # 1. Formato 2023 - 2026 (Ruta larga con año) con día de dos dígitos (DD)
    candidates.append(
        f"https://siea.midagri.gob.pe/files/files/datos_estadisticas/diarias/mml/{year}/{month_num}/sisap-ingreso-gmml-{day_2}{short_month}{short_year}.pdf"
    )
    
    # 2. Formato 2023 - 2026 con día de un dígito (D) si aplica
    if target_date.day < 10:
        candidates.append(
            f"https://siea.midagri.gob.pe/files/files/datos_estadisticas/diarias/mml/{year}/{month_num}/sisap-ingreso-gmml-{day_1}{short_month}{short_year}.pdf"
        )

    # 3. Formato 2019 - 2022 (Ruta corta sin año) con día de dos dígitos (DD)
    candidates.append(
        f"https://siea.midagri.gob.pe/files/datos_estadisticas/diarias/mml/{month_num}/sisap-ingreso-gmml-{day_2}{short_month}{short_year}.pdf"
    )

    # 4. Formato 2019 - 2022 con día de un dígito (D) si aplica
    if target_date.day < 10:
        candidates.append(
            f"https://siea.midagri.gob.pe/files/datos_estadisticas/diarias/mml/{month_num}/sisap-ingreso-gmml-{day_1}{short_month}{short_year}.pdf"
        )

    # 5. Formato alternativo para Septiembre (probando 'sep' en lugar de 'set')
    if target_date.month == 9:
        candidates.append(
            f"https://siea.midagri.gob.pe/files/files/datos_estadisticas/diarias/mml/{year}/{month_num}/sisap-ingreso-gmml-{day_2}sep{short_year}.pdf"
        )
        candidates.append(
            f"https://siea.midagri.gob.pe/files/datos_estadisticas/diarias/mml/{month_num}/sisap-ingreso-gmml-{day_2}sep{short_year}.pdf"
        )

    return candidates


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=False
)
def download_daily_pdf(target_date: date, timeout: int = 30) -> bytes | None:
    """
    Descarga el boletín diario en formato PDF para una fecha dada.
    Soporta 2016-2018 mediante crawler histórico, y 2019-2026 mediante descarga directa de SIEA.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/pdf,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3"
    }

    with httpx.Client(verify=False, follow_redirects=True, timeout=timeout) as client:
        # Caso 1: Rango Histórico (2016 - 2018)
        if 2016 <= target_date.year <= 2018:
            year = target_date.year
            if year not in _HISTORICAL_LINKS_CACHE:
                _HISTORICAL_LINKS_CACHE[year] = _crawl_historical_links_for_year(year)
                
            download_url = _HISTORICAL_LINKS_CACHE[year].get(target_date)
            if download_url:
                try:
                    logger.info(f"Descargando PDF histórico para la fecha {target_date} desde: {download_url}")
                    response = client.get(download_url, headers=headers)
                    if response.status_code == 200 and len(response.content) > 1000:
                        logger.info(f"¡Descarga exitosa de PDF histórico para la fecha {target_date}!")
                        return response.content
                except Exception as e:
                    logger.error(f"Error descargando PDF histórico desde {download_url}: {e}")
            else:
                logger.warning(f"No se encontró enlace histórico de descarga para la fecha: {target_date}")
            return None

        # Caso 2: Rango Moderno (2019 - 2026)
        candidates = _get_url_candidates(target_date)
        for url in candidates:
            try:
                logger.debug(f"Intentando descargar PDF desde: {url}")
                response = client.get(url, headers=headers)
                if response.status_code == 200 and len(response.content) > 1000:
                    logger.info(f"¡Descarga exitosa de PDF para la fecha {target_date} desde: {url}")
                    return response.content
                else:
                    logger.debug(f"No disponible en esta URL (Status: {response.status_code})")
            except Exception as e:
                logger.debug(f"Error en intento de descarga desde {url}: {e}")

    logger.warning(f"No se pudo encontrar un PDF válido de SIEA para la fecha: {target_date}")
    return None
