# Python 3.12 for selenium-wire proxy support (3.13+ can break pkg_resources)
# Build for amd64 (Chrome); on Apple Silicon use: docker compose build --platform linux/amd64
FROM python:3.12-slim-bookworm

# Install Chrome and dependencies for Selenium
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-key.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project and install
COPY pyproject.toml run.py .env.example ./
COPY src/ src/
COPY config/ config/
# setuptools<82 provides pkg_resources (removed in 82); selenium-wire needs it
RUN pip install --no-cache-dir "setuptools>=65,<82" && pip install --no-cache-dir -e .

# Chrome for headless Selenium
ENV CHROME_BIN=/usr/bin/google-chrome
ENV CHROMEDRIVER_AUTOINSTALL=1

# Default: run scraper
CMD ["python", "run.py"]
