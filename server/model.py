from enum import Enum
from pydantic import BaseModel, Field
from bson import ObjectId
from datetime import datetime


class Category(str, Enum):
    tourism = "du-lich"
    entertainment = "giai-tri"
    science_technology = "khoa-hoc-cong-nghe"
    car = "xe"
    news = "thoi-su"
    world = "the-gioi"
    sport = "the-thao"
    education = "giao-duc"
    health = "suc-khoe"
    economy = "kinh-doanh"
    latest = "moi-nhat"


class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, field):
        if not ObjectId.is_valid(v):
            raise ValueError(f"Invalid ObjectId: {v}")
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, schema, model_field) -> dict:
        """
        Generates a custom json schema for ObjectId,
        telling Pydantic that the type is string with UUID format.
        """
        return {"type": "string", "format": "uuid"}


class Article(BaseModel):
    id: PyObjectId = Field(alias='_id')
    thumbnail: str
    category: Category
    published_date: datetime
    title: str
    description: str
    content: list[str | list[str]]

    class Config:
        json_encoders = {ObjectId: str}


class ShortArticle(BaseModel):
    id: PyObjectId = Field(alias='_id')
    thumbnail: str
    title: str
    description: str

    class Config:
        json_encoders = {ObjectId: str}


class ArticleRecommendation(BaseModel):
    article: Article
    recommendations: list[ShortArticle]


class SearchResponse(BaseModel):
    articles: list[ShortArticle]
    total: int
