import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from sisap_light.config import get_settings


class SisapHttpClient:
    def __init__(self):
        settings = get_settings()
        self.timeout = settings.sisap_timeout_seconds
        self.retry_intentos = settings.sisap_retry_intentos

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def get(self, url: str, params: dict | None = None) -> str:
        logger.info("GET {url}", url=url)
        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            return response.text

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def post(self, url: str, data: dict | None = None) -> str:
        logger.info("POST {url}", url=url)
        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            response = client.post(url, data=data)
            response.raise_for_status()
            return response.text

