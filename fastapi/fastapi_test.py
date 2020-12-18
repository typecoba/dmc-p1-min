from fastapi import FastAPI
from typing import Optional
from pydantic import BaseModel
import logging


class Item(BaseModel):
    name: str
    description: Optional[str] = None
    price: float
    tax: Optional[float] = None


app = FastAPI()

# root test


@app.get('/')
async def root():
    logging.info("root test")
    return {"message": "Hello World"}

# path parameter


@app.get('/items/{item_id}')
def read_item(item_id: int, q: Optional[str] = None):
    if q:
        return {"item_id": item_id, "q": q}
    return {"item_id": item_id}

# request body basemodel


@app.post('/items/')
async def create_item(item: Item):
    return item
