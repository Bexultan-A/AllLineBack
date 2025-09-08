from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .db import Base, engine
from .routers import cities, corpuses, main_records

# Создаём таблицы (на старте). Для продакшена лучше Alembic, но ты просил просто и быстро.
Base.metadata.create_all(bind=engine)

app = FastAPI(title="VRTECH Address API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # при необходимости ограничить доменами фронта
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(cities.router)
app.include_router(corpuses.router)
app.include_router(main_records.router)

@app.get("/health")
def health():
    return {"status": "ok"}
