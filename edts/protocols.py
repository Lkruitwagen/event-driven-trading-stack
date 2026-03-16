from pydantic import BaseModel


class Message(BaseModel):
    generator: str | None
    strategy: str | None
    content: str | int | float
