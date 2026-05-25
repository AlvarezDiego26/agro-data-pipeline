import httpx
from loguru import logger
from tenacity import Retrying, stop_after_attempt, wait_exponential
from threading import Lock

from sisap_light.config import get_settings

_SHARED_CLIENT = None
_SHARED_CLIENT_LOCK = Lock()


class SisapHttpClient:
    def __init__(self):
        settings = get_settings()
        self.timeout = settings.sisap_timeout_seconds
        self.retry_intentos = settings.sisap_retry_intentos

        global _SHARED_CLIENT
        with _SHARED_CLIENT_LOCK:
            if _SHARED_CLIENT is None:
                # Usamos límites razonables para la reutilización de conexiones en el pool
                limits = httpx.Limits(max_keepalive_connections=20, max_connections=100)
                _SHARED_CLIENT = httpx.Client(
                    timeout=self.timeout,
                    follow_redirects=True,
                    limits=limits
                )

        self._client = _SHARED_CLIENT
        self.retry_policy = Retrying(
            stop=stop_after_attempt(max(int(self.retry_intentos or 1), 1)),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            reraise=True,
        )

    def get(self, url: str, params: dict | None = None) -> str:
        def _request() -> str:
            logger.info("GET {url}", url=url)
            response = self._client.get(url, params=params)
            response.raise_for_status()
            return response.text

        return self.retry_policy(_request)

    def post(self, url: str, data: dict | None = None) -> str:
        def _request() -> str:
            logger.info("POST {url}", url=url)
            response = self._client.post(url, data=data)
            response.raise_for_status()
            return response.text

        return self.retry_policy(_request)

    def close(self) -> None:
        # El cliente compartido no se cierra para que otros hilos puedan seguir usándolo.
        # Se liberará automáticamente al terminar el proceso de ejecución de la CLI.
        pass

    def __del__(self):
        pass

