import asyncio
import os
import signal
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from edts.common import get_logger
from edts.protocols import Message


class Trader:
    """Trader model representing the trade execution logic."""

    def __init__(self, _id: str = "trader"):
        self._id = _id
        self._cache: list[str] = []
        self.logger = get_logger(f"{type(self).__module__}.{self._id}")

    def execute_trades(self) -> dict:
        tick_time = datetime.now()

        if not self._cache:
            self.logger.info(f"No signals received at {tick_time.isoformat()}, skipping.")
            return {"status": "no_signals"}

        counts = Counter(self._cache)
        mode_signal = counts.most_common(1)[0][0]
        self._cache.clear()

        self.logger.info(
            f"Executing trades at {tick_time.isoformat()}: \n"
            + f"mode signal = '{mode_signal}' (counts: {dict(counts)})"
        )
        return {"status": "success", "signal": mode_signal}

    def make_app(self) -> FastAPI:
        scheduler = AsyncIOScheduler()

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            self.logger.info("Starting Trader service...")
            scheduler.add_job(self.execute_trades, "interval", seconds=5)
            scheduler.start()
            yield
            scheduler.shutdown()
            self.logger.info("Shutting down Trader service...")

        app = FastAPI(lifespan=lifespan)

        @app.post("/message")
        async def receive_message(message: Message):
            self._cache.append(str(message.content))
            return {"status": "ok"}

        @app.get("/health")
        async def health():
            return {"status": "healthy"}

        @app.post("/shutdown")
        async def shutdown():
            async def _shutdown():
                await asyncio.sleep(0.5)  # let response return first
                os.kill(os.getpid(), signal.SIGTERM)

            asyncio.create_task(_shutdown())
            return {"message": "Shutdown initiated."}

        return app


trader = Trader()
app = trader.make_app()
