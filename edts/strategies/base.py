import asyncio
import os
import signal
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager

import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from edts.common import get_logger
from edts.protocols import Message


class Strategy(ABC):
    """
    Base class for trading strategies.

    Lifecycle note: after this service is running, an external client must:
      1. Subscribe this strategy's /message endpoint to the input topic on the pubsub.
         e.g. POST {pubsub_url}/subscribe/{subscribe_topic}?subscriber={self_url}/message
      2. Ensure the publish topic exists on the pubsub.
         e.g. POST {pubsub_url}/topic/{publish_topic}
    """

    def __init__(
        self,
        pubsub_url: str,
        publish_topic: str,
        self_url: str,
        interval_seconds: int = 5,
        name: str | None = None,
    ):
        self.pubsub_url = pubsub_url.rstrip("/")
        self.publish_topic = publish_topic
        self.self_url = self_url.rstrip("/")
        self.interval_seconds = interval_seconds
        self.logger = get_logger(f"{type(self).__module__}.{name or type(self).__name__}")

    @abstractmethod
    def handle_message(self, message: Message) -> None:
        """Update internal state from an incoming price message."""
        ...

    @abstractmethod
    def process_signal(self) -> Message | None:
        """Compute and return a signal to publish, or None to suppress."""
        ...

    def _tick(self):
        signal = self.process_signal()
        if signal is not None:
            r = requests.post(
                f"{self.pubsub_url}/publish/{self.publish_topic}",
                json=signal.model_dump(),
            )
            r.raise_for_status()
            self.logger.info(f"Published signal: {signal.model_dump_json()[:60]}...")

    def make_app(self) -> FastAPI:
        scheduler = AsyncIOScheduler()

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            self.logger.info(f"Starting Strategy service ({self.__class__.__name__})...")
            scheduler.add_job(self._tick, "interval", seconds=self.interval_seconds)
            scheduler.start()
            yield
            scheduler.shutdown()
            self.logger.info("Shutting down Strategy service...")

        app = FastAPI(lifespan=lifespan)

        @app.post("/message")
        async def receive_message(message: Message):
            self.handle_message(message)
            return {"status": "ok"}

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
