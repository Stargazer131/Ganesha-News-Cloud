import asyncio
from typing import Annotated
from fastapi import FastAPI, Query, HTTPException
from server.model import Article, Category, ArticleRecommendation, ShortArticle, PyObjectId, SearchResponse
from server.data import load_neighbor_graph, connect_to_mongo
from server.updater import update_new_articles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import re


async def periodic_task():
    global neighbor_graph
    await asyncio.sleep(5)
    while True:
        neighbor_graph = await asyncio.to_thread(update_new_articles, limit=5)
        await asyncio.sleep(60 * 60 * 24)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global database, neighbor_graph
    client = connect_to_mongo()
    database = client["Ganesha_News"]
    neighbor_graph = load_neighbor_graph()
    # asyncio.create_task(periodic_task())

    yield
    client.close()


app = FastAPI(lifespan=lifespan)

origins = [
    "http://localhost:3000",
    "https://recently-profound-crab.ngrok-free.app",
    "https://stargazer131.github.io",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/articles", response_model=list[ShortArticle])
def get_articles_by_category(
    page: Annotated[int, Query(ge=1, le=20)] = 1,
    limit: Annotated[int, Query(ge=10, le=40)] = 20,
    category: Category = Category.latest
):
    query = {}
    fields = {"title": 1, "description": 1, "thumbnail": 1}
    sort_criteria = {"published_date": -1}
    if category != Category.latest:
        query = {"category": category}
    
    articles = database['newspaper'].find(query, fields).sort(sort_criteria).skip((page - 1) * limit).limit(limit)
    return [ShortArticle(**article) for article in articles]


@app.get("/article/{article_id}", response_model=ArticleRecommendation)
def get_article_and_recommendations_by_id(
    article_id: PyObjectId, 
    limit: Annotated[int, Query(ge=5, le=20)] = 10,
):    
    article = database['newspaper'].find_one({"_id": article_id})
    if article is None:
        raise HTTPException(404, "Article not found")
    
    res_index = neighbor_graph[article['index']]
    filter_index = res_index.astype(int).tolist()[1 : limit + 1]
    query = {"index": {"$in": filter_index}}
    fields = {"title": 1, "description": 1, "thumbnail": 1}

    recommendation_list = database['newspaper'].find(query, fields)
    article = Article(**article)
    recommendations = [ShortArticle(**item) for item in recommendation_list]
    return ArticleRecommendation(article=article, recommendations=recommendations)


@app.get("/search", response_model=SearchResponse)
def get_articles_by_keyword(
    keyword: str,
    limit: Annotated[int, Query(ge=1, le=50)] = 30,
    page: Annotated[int, Query(ge=1, le=50)] = 1,
):
    regex_pattern = re.compile(
        fr"(?:\s+[“'\"]?{keyword}[”'\"]?$|^[“'\"]?{keyword}[”'\"]?\s+|\s+[“'\"]?{keyword}[”'\"]?\s+)", re.IGNORECASE
    )

    query = {
        "$or": [
            {"title": {"$regex": regex_pattern}},
            {"description": {"$regex": regex_pattern}}
        ]
    }
    fields = {"title": 1, "description": 1, "thumbnail": 1}
    sort_criteria = {"published_date": -1}

    combined_articles = list(database['newspaper'].find(query, fields).sort(sort_criteria))
    start_index = min(len(combined_articles) - 1, (page - 1) * limit)
    end_index = min(len(combined_articles), page * limit)

    articles = [ShortArticle(**article) for article in combined_articles[start_index : end_index]]
    return SearchResponse(articles=articles, total=min(len(combined_articles), limit * 50))

