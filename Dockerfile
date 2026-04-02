FROM python:3.12-slim-bookworm

# Install Chromium dependencies for Playwright
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
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project and install
COPY pyproject.toml run.py ./
COPY src/ src/
COPY config/ config/
RUN pip install --no-cache-dir -e .

# Install Playwright browsers
RUN playwright install --with-deps chromium

EXPOSE 8000

# Default: run API server
CMD ["uvicorn", "ali_scraper.api:app", "--host", "0.0.0.0", "--port", "8000"]
