"""Base scraper class with browser setup and common operations."""

import sys
import time
from typing import Optional

from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from ..config import settings
from ..utils import get_user_agent

_selenium_wire_error: str | None = None
try:
    from seleniumwire import webdriver as wire_webdriver
    SELENIUM_WIRE_AVAILABLE = True
except Exception as e:
    SELENIUM_WIRE_AVAILABLE = False
    wire_webdriver = None  # type: ignore
    _selenium_wire_error = str(e)

_proxy_warned = [False]  # log proxy warning only once per process

try:
    from ..proxy import proxy_manager
    PROXY_MANAGER_AVAILABLE = True
except ImportError:
    PROXY_MANAGER_AVAILABLE = False
    proxy_manager = None  # type: ignore


# Scraping constants (in code, not env)
BROWSER_TIMEOUT = 20  # seconds to wait for page elements


class BaseScraper:
    """Base class for all scrapers - handles browser setup and common operations."""

    def __init__(
        self,
        headless: bool = True,
        use_proxy: Optional[bool] = None,
    ) -> None:
        self.headless = headless
        self.use_proxy = use_proxy if use_proxy is not None else settings.use_proxy
        self.driver: Optional[webdriver.Chrome] = None
        self.wait: Optional[WebDriverWait] = None

    def init_driver(self) -> webdriver.Chrome:
        """Setup Chrome browser with anti-detection settings and optional proxy."""
        chrome_options = Options()

        # Run Chrome without visible window (headless = no browser UI)
        if self.headless:
            chrome_options.add_argument("--headless=new")

        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_experimental_option(
            "prefs",
            {"profile.managed_default_content_settings.images": 2},  # 2 = block images (faster loads)
        )
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument(f"user-agent={get_user_agent()}")
        chrome_options.add_argument("--window-size=1920,1080")

        service = Service(ChromeDriverManager().install())

        if self.use_proxy and SELENIUM_WIRE_AVAILABLE and PROXY_MANAGER_AVAILABLE and proxy_manager and proxy_manager.api_key:
            proxy_config = proxy_manager.get_selenium_proxy_config()
            if proxy_config:
                seleniumwire_options = {
                    "proxy": {
                        "http": f"http://{proxy_config['username']}:{proxy_config['password']}@{proxy_config['host']}:{proxy_config['port']}",
                        "https": f"http://{proxy_config['username']}:{proxy_config['password']}@{proxy_config['host']}:{proxy_config['port']}",
                        "no_proxy": "localhost,127.0.0.1",
                    }
                }
                self.driver = wire_webdriver.Chrome(
                    service=service,
                    options=chrome_options,
                    seleniumwire_options=seleniumwire_options,
                )
                # logger.info(f"Browser initialized with proxy: {proxy_config['host']}:{proxy_config['port']}")
            else:
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.warning("Proxy enabled but Webshare returned no proxies; running without proxy")
        else:
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            if self.use_proxy and not SELENIUM_WIRE_AVAILABLE:
                if not _proxy_warned[0]:
                    _proxy_warned[0] = True
                    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}"
                    err = f": {_selenium_wire_error}" if _selenium_wire_error else ""
                    logger.warning(
                        "Proxy enabled but selenium-wire failed to import (Python %s)%s. "
                        "Set USE_PROXY=false in .env to suppress. Running without proxy.",
                        py_ver,
                        err,
                    )
            else:
                pass  # logger.info("Browser initialized without proxy")

        self.wait = WebDriverWait(self.driver, BROWSER_TIMEOUT)
        self.driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.info("Browser initialized successfully")
        return self.driver

    def get_page(self, url: str) -> BeautifulSoup:
        """Load a page and return parsed HTML."""
        self.driver.get(url)
        time.sleep(3)
        return BeautifulSoup(self.driver.page_source, "lxml")

    def wait_for_element(self, by: By, value: str, timeout: Optional[int] = None):
        """Wait for an element to appear on the page."""
        wait_time = timeout or BROWSER_TIMEOUT
        return WebDriverWait(self.driver, wait_time).until(
            EC.presence_of_element_located((by, value))
        )

    def scroll_page(self, times: int = 3) -> None:
        """Scroll down the page to load dynamic content."""
        for _ in range(times):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)

    def close(self) -> None:
        """Close the browser."""
        if self.driver:
            self.driver.quit()
            logger.info("Browser closed")

    def __enter__(self) -> "BaseScraper":
        self.init_driver()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
