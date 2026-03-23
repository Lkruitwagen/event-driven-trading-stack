import os
import random

from edts.generators.base import Generator
from edts.protocols import Message


class RandomWalkMeanReversionGenerator(Generator):
    def __init__(
        self,
        topic_url: str,
        interval_seconds: int = 5,
        initial_value: float = 100.0,
        mu: float = 100.0,
        theta: float = 0.1,
        sigma: float = 1.0,
        name: str | None = None,
    ):
        super().__init__(topic_url, interval_seconds, name)
        self.value = initial_value
        self.mu = mu
        self.theta = theta
        self.sigma = sigma

    def generate(self) -> Message:
        drift = self.theta * (self.mu - self.value)
        noise = self.sigma * random.gauss(0, 1)
        self.value += drift + noise
        return Message(generator="random_walk", strategy=None, content=self.value)


gen = RandomWalkMeanReversionGenerator(
    topic_url=os.environ["TOPIC_URL"],
    interval_seconds=int(os.environ.get("INTERVAL_SECONDS", "5")),
    initial_value=float(os.environ.get("INITIAL_VALUE", "100.0")),
    mu=float(os.environ.get("MU", "100.0")),
    theta=float(os.environ.get("THETA", "0.1")),
    sigma=float(os.environ.get("SIGMA", "1.0")),
    name=os.environ.get("NAME", "random_walk"),
)
app = gen.make_app()
