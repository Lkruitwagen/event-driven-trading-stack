import asyncio
import os
import signal
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from urllib.parse import urlparse

import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from edts.common import get_logger
from edts.protocols import Message

logger = get_logger(__name__)


class Generator(ABC):
    def __init__(self, topic_url: str, interval_seconds: int = 5):
        self.topic_url = topic_url
        self.interval_seconds = interval_seconds

    @abstractmethod
    def generate(self) -> Message: ...

    def _register_topic(self):
        parsed = urlparse(self.topic_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        topic = parsed.path.rstrip("/").split("/")[-1]
        r = requests.post(f"{base_url}/topic/{topic}")
        r.raise_for_status()
        logger.info(f"Registered topic '{topic}' on {base_url}")

    def _tick(self):
        message = self.generate()
        r = requests.post(self.topic_url, json=message.model_dump())
        r.raise_for_status()
        logger.info(f"Published message to {self.topic_url}: {message.model_dump_json()[:40]}...")

    def make_app(self) -> FastAPI:
        scheduler = AsyncIOScheduler()

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            logger.info(f"Starting Generator service ({self.__class__.__name__})...")
            self._register_topic()
            scheduler.add_job(self._tick, "interval", seconds=self.interval_seconds)
            scheduler.start()
            yield
            scheduler.shutdown()
            logger.info("Shutting down Generator service...")

        app = FastAPI(lifespan=lifespan)

        @app.get("/health")
        async def health():
            return {"status": "healthy"}

        @app.post("/shutdown")
        async def shutdown():
            async def _shutdown():
                await asyncio.sleep(0.5)
                os.kill(os.getpid(), signal.SIGTERM)

            asyncio.create_task(_shutdown())
            return {"message": "Shutdown initiated."}

        return app
