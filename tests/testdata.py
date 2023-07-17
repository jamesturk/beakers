from pydantic import BaseModel


class Word(BaseModel):
    word: str
