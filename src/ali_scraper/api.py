"""FastAPI server exposing the scraping pipeline as a REST API."""

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from enum import Enum

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .cli import scrape_category, setup_logging
from .config import settings

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="AliExpress Scraper API",
    version="1.0.0",
    description="Scrape AliExpress products by query, upload images to Cloudflare, store in MongoDB.",
)

# Thread pool for background scraping jobs
_executor = ThreadPoolExecutor(max_workers=2)
_jobs: dict[str, dict] = {}


# ---------- Models ----------

class ScrapeRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=200, examples=["wireless earbuds"])
    max_pages: int = Field(default=1, ge=1, le=int(settings.max_pages))
    download_images: bool = True
    upload_cloudflare: bool = True
    store_db: bool = True


class JobStatus(str, Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class ScrapeResponse(BaseModel):
    job_id: str
    status: JobStatus
    message: str


class JobResult(BaseModel):
    job_id: str
    status: JobStatus
    query: str | None = None
    total_products: int = 0
    products: list[dict] = []
    error: str | None = None


# ---------- Background runner ----------

def _run_scrape(job_id: str, req: ScrapeRequest):
    _jobs[job_id]["status"] = JobStatus.running
    try:
        products = scrape_category(
            query=req.query,
            pages=req.max_pages,
            download_images=req.download_images,
            upload_cloudflare=req.upload_cloudflare,
            store_db=req.store_db,
        )
        _jobs[job_id]["status"] = JobStatus.completed
        _jobs[job_id]["total_products"] = len(products)
        _jobs[job_id]["products"] = products
    except Exception as e:
        logger.exception(f"Job {job_id} failed")
        _jobs[job_id]["status"] = JobStatus.failed
        _jobs[job_id]["error"] = str(e)


# ---------- Endpoints ----------

@app.post("/scrape", response_model=ScrapeResponse)
def start_scrape(req: ScrapeRequest):
    """Start a scraping job in the background.

    Pass `query` (product name) and `max_pages` to control the scrape.
    Returns a `job_id` you can poll with GET /scrape/{job_id}.
    """
    job_id = uuid.uuid4().hex[:12]
    _jobs[job_id] = {
        "status": JobStatus.pending,
        "query": req.query,
        "total_products": 0,
        "products": [],
        "error": None,
    }
    _executor.submit(_run_scrape, job_id, req)
    return ScrapeResponse(
        job_id=job_id,
        status=JobStatus.pending,
        message=f"Scraping '{req.query}' ({req.max_pages} page(s)) — poll GET /scrape/{job_id}",
    )


@app.post("/scrape/sync", response_model=JobResult)
def scrape_sync(req: ScrapeRequest):
    """Run a scraping job synchronously and return results directly.

    Use this for quick scrapes (1-2 pages). For larger jobs use POST /scrape.
    """
    job_id = uuid.uuid4().hex[:12]
    try:
        products = scrape_category(
            query=req.query,
            pages=req.max_pages,
            download_images=req.download_images,
            upload_cloudflare=req.upload_cloudflare,
            store_db=req.store_db,
        )
        return JobResult(
            job_id=job_id,
            status=JobStatus.completed,
            query=req.query,
            total_products=len(products),
            products=products,
        )
    except Exception as e:
        logger.exception("Sync scrape failed")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/scrape/{job_id}", response_model=JobResult)
def get_job(job_id: str):
    """Check the status and results of a scraping job."""
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return JobResult(
        job_id=job_id,
        status=job["status"],
        query=job.get("query"),
        total_products=job["total_products"],
        products=job["products"],
        error=job.get("error"),
    )


@app.get("/health")
def health():
    return {"status": "ok"}
