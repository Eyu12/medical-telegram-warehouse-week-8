from pydantic import BaseModel
from typing import List

class TopProductsResponse(BaseModel):
    detected_class: str
    count: int

class ChannelActivityResponse(BaseModel):
    date_key: str
    posts: int

class MessageResponse(BaseModel):
    message_id: str
    content: str

class VisualContentStatsResponse(BaseModel):
    channel_key: str
    image_count: int
