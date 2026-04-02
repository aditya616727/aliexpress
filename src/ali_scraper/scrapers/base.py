"""Base scraper with Playwright browser management and anti-detection."""

import logging
import random

from playwright.sync_api import sync_playwright

from ..config import settings

logger = logging.getLogger(__name__)

# Rotate through realistic Chrome UAs to reduce fingerprinting
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

# Comprehensive stealth script to hide automation markers
_STEALTH_JS = """
    // Remove webdriver flag
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    delete navigator.__proto__.webdriver;

    // Fake plugins array
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const arr = [
                {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer'},
                {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai'},
                {name: 'Native Client', filename: 'internal-nacl-plugin'},
            ];
            arr.item = i => arr[i];
            arr.namedItem = n => arr.find(p => p.name === n);
            arr.refresh = () => {};
            return arr;
        }
    });

    // Languages
    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});

    // Chrome runtime object
    window.chrome = {
        runtime: {id: undefined, connect: function(){}, sendMessage: function(){}},
        loadTimes: function(){ return {}; },
        csi: function(){ return {}; },
    };

    // Hide automation-related properties
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) =>
        parameters.name === 'notifications'
            ? Promise.resolve({state: Notification.permission})
            : originalQuery(parameters);

    // Spoof hardware concurrency and device memory
    Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
    Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});

    // Override toString to hide native code modifications
    const nativeToString = Function.prototype.toString;
    Function.prototype.toString = function() {
        if (this === Function.prototype.toString) return 'function toString() { [native code] }';
        return nativeToString.call(this);
    };
"""


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
                "--disable-infobars",
                "--window-size=1920,1080",
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
            user_agent=random.choice(_USER_AGENTS),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            java_script_enabled=True,
            **extra_kwargs,
        )
        if settings.proxy_server:
            context_kwargs["proxy"] = {"server": settings.proxy_server}
            logger.info(f"Using proxy: {settings.proxy_server}")

        context = browser.new_context(**context_kwargs)
        context.add_init_script(_STEALTH_JS)

        return context

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._close_browser()
