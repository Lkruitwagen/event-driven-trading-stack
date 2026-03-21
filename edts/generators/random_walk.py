import os
import random

from edts.generators.base import Generator
from edts.protocols import Message


class RandomWalkGenerator(Generator):
    def __init__(
        self,
        topic_url: str,
        interval_seconds: int = 5,
        initial_value: float = 100.0,
        step_size: float = 1.0,
    ):
        super().__init__(topic_url, interval_seconds)
        self.value = initial_value
        self.step_size = step_size

    def generate(self) -> Message:
        self.value += random.uniform(-self.step_size, self.step_size)
        return Message(generator="random_walk", strategy=None, content=self.value)


gen = RandomWalkGenerator(
    topic_url=os.environ["TOPIC_URL"],
    interval_seconds=int(os.environ.get("INTERVAL_SECONDS", "5")),
    initial_value=float(os.environ.get("INITIAL_VALUE", "100.0")),
    step_size=float(os.environ.get("STEP_SIZE", "1.0")),
)
app = gen.make_app()
