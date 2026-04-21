"""Webshare proxy manager."""

import threading

# Residential/backbone: proxy_address can be None – use rotating gateway
WEBSHARE_GATEWAY = "p.webshare.io"
import time
from typing import Any, Dict, List, Optional

import requests
from loguru import logger

from ..config import settings


class ProxyManager:
    """Manages Webshare proxy connections and rotation."""

    def __init__(self) -> None:
        self.api_key = settings.webshare_api_key
        self.proxy_list: List[Dict[str, Any]] = []
        self.current_index = 0
        self._fetch_lock = threading.Lock()
        self._last_fetch_failed_at: Optional[float] = None
        self._fetch_cooldown_sec = 60  # avoid hammering API after failure

    def _detect_proxy_mode(self, headers: Dict[str, str]) -> Optional[str]:
        """Try direct first; use backbone if residential (direct not allowed)."""
        for mode in ("direct", "backbone"):
            try:
                url = f"https://proxy.webshare.io/api/v2/proxy/list/?mode={mode}&page=1&page_size=1"
                r = requests.get(url, headers=headers, timeout=15)
                if r.status_code == 200:
                    return mode
                if r.status_code == 400 and mode == "direct":
                    continue  # try backbone for residential plans
            except Exception:
                pass
        self._last_fetch_failed_at = time.time()
        logger.error("Could not fetch proxies. Try direct or backbone mode manually.")
        return None

    def fetch_proxy_list(self) -> List[Dict[str, Any]]:
        """Fetch available proxies from Webshare API."""
        with self._fetch_lock:
            if not self.api_key:
                logger.warning("No Webshare API key configured")
                return []

            # Avoid hammering API when multiple workers fail at once
            if self._last_fetch_failed_at and (time.time() - self._last_fetch_failed_at) < self._fetch_cooldown_sec:
                return []

            try:
                headers = {"Authorization": f"Token {self.api_key}"}
                # Residential plans require mode=backbone; datacenter plans use mode=direct
                mode = self._detect_proxy_mode(headers)
                if not mode:
                    return []

                all_proxies: List[Dict[str, Any]] = []
                page = 1
                page_size = 25

                while True:
                    url = f"https://proxy.webshare.io/api/v2/proxy/list/?mode={mode}&page={page}&page_size={page_size}"
                    response = requests.get(url, headers=headers, timeout=15)

                    if response.status_code != 200:
                        self._last_fetch_failed_at = time.time()
                        body = response.text[:500] if response.text else "(empty)"
                        logger.error(
                            "Failed to fetch proxies: %s. Response: %s",
                            response.status_code,
                            body,
                        )
                        return []

                    data = response.json()
                    results = data.get("results", [])
                    all_proxies.extend(results)

                    if not data.get("next"):
                        break
                    page += 1
                    if page > 10:
                        break

                self._last_fetch_failed_at = None
                self.proxy_list = all_proxies
                logger.info(f"Fetched {len(self.proxy_list)} proxies from Webshare ({mode})")
                return self.proxy_list
            except Exception as e:
                self._last_fetch_failed_at = time.time()
                logger.error(f"Error fetching proxy list: {e}")
                return []

    def _get_proxy_host_port(self, proxy: Dict[str, Any]) -> tuple:
        """Residential/backbone may have proxy_address=None – use gateway."""
        host = proxy.get("proxy_address")
        port = proxy.get("port")
        if not host:
            host = WEBSHARE_GATEWAY
        return host, port

    def get_proxy_config(self) -> Optional[Dict[str, str]]:
        """Get proxy configuration from Webshare API."""
        if not self.proxy_list:
            self.fetch_proxy_list()
        if not self.proxy_list:
            return None
        proxy = self.proxy_list[self.current_index % len(self.proxy_list)]
        self.current_index += 1
        host, port = self._get_proxy_host_port(proxy)
        proxy_url = f"http://{proxy['username']}:{proxy['password']}@{host}:{port}"
        return {"http": proxy_url, "https": proxy_url}

    def get_selenium_proxy_config(self) -> Optional[Dict[str, Any]]:
        """Get proxy configuration for Selenium from Webshare API."""
        if not self.proxy_list:
            self.fetch_proxy_list()
        if not self.proxy_list:
            return None
        proxy = self.proxy_list[self.current_index % len(self.proxy_list)]
        self.current_index += 1
        host, port = self._get_proxy_host_port(proxy)
        return {
            "host": host,
            "port": port,
            "username": proxy["username"],
            "password": proxy["password"],
        }

    def test_proxy(self, proxy_config: Optional[Dict[str, str]] = None) -> bool:
        """Test if proxy is working."""
        try:
            config = proxy_config or self.get_proxy_config()
            if not config:
                return False
            response = requests.get("https://ipv4.webshare.io/", proxies=config, timeout=10)
            if response.status_code == 200:
                logger.info(f"Proxy test successful. IP: {response.text.strip()}")
                return True
            return False
        except Exception as e:
            logger.error(f"Proxy test error: {e}")
            return False

    def rotate_proxy(self) -> None:
        """Manually rotate to next proxy."""
        if self.proxy_list:
            self.current_index = (self.current_index + 1) % len(self.proxy_list)


proxy_manager = ProxyManager()
