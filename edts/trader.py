import asyncio
import os
import signal
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from edts.common import get_logger

logger = get_logger(__name__)
scheduler = AsyncIOScheduler()


class Trader:
    """Trader model representing the trade execution logic."""

    def __init__(self, _id: str = "trader"):
        self._id = _id  # Unique identifier for the trader

    def execute_trades(self) -> dict:
        """Execute a trade based on the given trade signal."""
        tick_time = datetime.now()
        # Implement trade execution logic here

        logger.info(f"Executing trades at {tick_time.isoformat()}...")
        return {"status": "success"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Trader service...")
    app.state.trader = Trader()
    scheduler.add_job(app.state.trader.execute_trades, "interval", seconds=5)
    scheduler.start()
    yield
    scheduler.shutdown()
    logger.info("Shutting down Trader service...")


app = FastAPI(lifespan=lifespan)


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
