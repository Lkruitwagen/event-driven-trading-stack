import os

from edts.protocols import Message
from edts.strategies.base import Strategy


class MeanReversionStrategy(Strategy):
    """
    Computes a running mean of received prices. On each signal tick, emits
    'buy' if the latest price is below the long-run mean, 'sell' if above.
    """

    def __init__(
        self,
        pubsub_url: str,
        publish_topic: str,
        self_url: str,
        interval_seconds: int = 5,
        name: str | None = None,
    ):
        super().__init__(pubsub_url, publish_topic, self_url, interval_seconds, name)
        self._count = 0
        self._mean = 0.0
        self._latest: float | None = None

    def handle_message(self, message: Message) -> None:
        price = float(message.content)
        self._latest = price
        # Welford's online mean update
        self._count += 1
        self._mean += (price - self._mean) / self._count

    def process_signal(self) -> Message | None:
        if self._latest is None or self._count < 2:
            return None

        action = "buy" if self._latest < self._mean else "sell"
        return Message(generator=None, strategy="mean_reversion", content=action)


strategy = MeanReversionStrategy(
    pubsub_url=os.environ["PUBSUB_URL"],
    publish_topic=os.environ["PUBLISH_TOPIC"],
    self_url=os.environ["SELF_URL"],
    interval_seconds=int(os.environ.get("INTERVAL_SECONDS", "5")),
    name=os.environ.get("NAME", "mean_reversion"),
)
app = strategy.make_app()
