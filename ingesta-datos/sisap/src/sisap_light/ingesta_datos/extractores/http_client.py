import httpx
from loguru import logger
from tenacity import Retrying, stop_after_attempt, wait_exponential

from sisap_light.config import get_settings


class SisapHttpClient:
    def __init__(self):
        settings = get_settings()
        self.timeout = settings.sisap_timeout_seconds
        self.retry_intentos = settings.sisap_retry_intentos
        self.retry_policy = Retrying(
            stop=stop_after_attempt(max(int(self.retry_intentos or 1), 1)),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            reraise=True,
        )

    def get(self, url: str, params: dict | None = None) -> str:
        def _request() -> str:
            logger.info("GET {url}", url=url)
            with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                response = client.get(url, params=params)
                response.raise_for_status()
                return response.text

        return self.retry_policy(_request)

    def post(self, url: str, data: dict | None = None) -> str:
        def _request() -> str:
            logger.info("POST {url}", url=url)
            with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                response = client.post(url, data=data)
                response.raise_for_status()
                return response.text

        return self.retry_policy(_request)

