"""Base scraper with Playwright browser management and anti-detection."""

import logging

from playwright.sync_api import sync_playwright

from ..config import settings

logger = logging.getLogger(__name__)


class BaseScraper:
    """Base scraper providing Playwright browser setup with stealth measures.

    Subclasses implement site-specific scraping logic.
    """

    def __init__(self):
        self._pw = None
        self._browser = None

    def _launch_browser(self):
        """Launch Playwright Chromium with anti-detection options."""
        if self._browser is None:
            self._pw = sync_playwright().start()
            launch_args = [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ]
            if not settings.chrome_sandbox:
                launch_args.append("--no-sandbox")
            self._browser = self._pw.chromium.launch(
                headless=settings.headless,
                args=launch_args,
            )
        return self._browser

    def _close_browser(self):
        """Safely close browser and Playwright."""
        if self._browser:
            self._browser.close()
            self._browser = None
        if self._pw:
            self._pw.stop()
            self._pw = None

    def _create_context(self, **extra_kwargs):
        """Create a new browser context with stealth and optional proxy."""
        browser = self._launch_browser()

        context_kwargs = dict(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            java_script_enabled=True,
            **extra_kwargs,
        )
        if settings.proxy_server:
            context_kwargs["proxy"] = {"server": settings.proxy_server}
            logger.info(f"Using proxy: {settings.proxy_server}")

        context = browser.new_context(**context_kwargs)

        # Hide automation markers from anti-bot detection
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            delete navigator.__proto__.webdriver;
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){} };
        """)

        return context

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._close_browser()
