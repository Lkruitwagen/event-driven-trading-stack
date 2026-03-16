from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI, Request

from edts.common import get_logger
from edts.protocols import Message

logger = get_logger(__name__)


class PubSub:
    """
    Simple in-memory Pub/Sub system.
    """

    def __init__(self):
        self.topics = {}  # topic: set of subscriber ids
        self.subscribers = {}  # id: url

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
        for subscriber_id in self.topics[topic]:
            r = requests.post(self.subscribers[subscriber_id], json=message.model_dump())
            r.raise_for_status()
            logger.info(
                f"""Topic {topic}: 
                    Published message to subscriber {subscriber_id}: 
                        {message.model_dump_json()[0:20]}...
                """
            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Pub/Sub service...")
    app.state.pubsub = PubSub()
    yield
    logger.info("Shutting down Pub/Sub service...")


app = FastAPI(lifespan=lifespan)


@app.post("/topic/{topic}")
async def new_topic(topic: str, request: Request):
    request.app.state.pubsub.new_topic(topic)
    return {"message": f"Topic '{topic}' created."}


@app.post("/subscribe/{topic}")
async def subscribe(topic: str, subscriber: str, request: Request):
    request.app.state.pubsub.subscribe(topic, subscriber)
    return {"message": f"Subscriber '{subscriber}' subscribed to topic '{topic}'."}


@app.post("/unsubscribe/{topic}")
async def unsubscribe(topic: str, subscriber: str, request: Request):
    request.app.state.pubsub.unsubscribe(topic, subscriber)
    return {"message": f"Subscriber '{subscriber}' unsubscribed from topic '{topic}'."}


@app.get("/status")
async def status(request: Request):
    pubsub = request.app.state.pubsub
    return {
        "topics": list(pubsub.topics.keys()),
        "subscribers": list(pubsub.subscribers.keys()),
        "subscriptions": {topic: list(subs) for topic, subs in pubsub.topics.items()},
    }


@app.post("/publish/{topic}")
async def publish(topic: str, message: Message, request: Request):
    request.app.state.pubsub.publish(topic, message)
    return {"message": f"Message published to topic '{topic}'."}
