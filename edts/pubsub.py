import asyncio
import os
import signal
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI

from edts.common import get_logger
from edts.protocols import Message


class PubSub:
    """
    Simple in-memory Pub/Sub system.
    """

    def __init__(self, name: str = "pubsub"):
        self.topics = {}  # topic: set of subscriber ids
        self.subscribers = {}  # id: url
        self.logger = get_logger(f"{type(self).__module__}.{name}")

    def new_topic(self, topic):
        if topic not in self.topics:
            self.topics[topic] = set()

    def subscribe(self, topic, subscriber):
        if topic not in self.topics:
            raise ValueError(f"Topic '{topic}' does not exist.")

        self.topics[topic].add(subscriber)
        self.subscribers[subscriber] = subscriber

    def unsubscribe(self, topic, subscriber):
        if topic in self.topics:
            self.topics[topic].discard(subscriber)

    def publish(self, topic: str, message: Message):
        if topic not in self.topics:
            raise ValueError(f"Topic '{topic}' does not exist.")

        self.logger.info(
            f"""Publishing message {message.model_dump_json()[0:50]}... \n
            to topic '{topic}' \n with subscribers {self.topics[topic]}
            """
        )

        for subscriber_id in self.topics[topic]:
            r = requests.post(self.subscribers[subscriber_id], json=message.model_dump())
            r.raise_for_status()

    def make_app(self) -> FastAPI:
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            self.logger.info("Starting Pub/Sub service...")
            yield
            self.logger.info("Shutting down Pub/Sub service...")

        app = FastAPI(lifespan=lifespan)

        @app.post("/topic/{topic}")
        async def new_topic(topic: str):
            self.new_topic(topic)
            return {"message": f"Topic '{topic}' created."}

        @app.post("/subscribe/{topic}")
        async def subscribe(topic: str, subscriber: str):
            self.subscribe(topic, subscriber)
            return {"message": f"Subscriber '{subscriber}' subscribed to topic '{topic}'."}

        @app.post("/unsubscribe/{topic}")
        async def unsubscribe(topic: str, subscriber: str):
            self.unsubscribe(topic, subscriber)
            return {"message": f"Subscriber '{subscriber}' unsubscribed from topic '{topic}'."}

        @app.get("/status")
        async def status():
            return {
                "topics": list(self.topics.keys()),
                "subscribers": list(self.subscribers.keys()),
                "subscriptions": {topic: list(subs) for topic, subs in self.topics.items()},
            }

        @app.post("/publish/{topic}")
        async def publish(topic: str, message: Message):
            self.publish(topic, message)
            return {"message": f"Message published to topic '{topic}'."}

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


pubsub = PubSub(name=os.environ.get("NAME", "pubsub"))
app = pubsub.make_app()
