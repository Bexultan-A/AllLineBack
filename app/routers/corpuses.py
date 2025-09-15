from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import asc, case, desc, func, select, and_
from ..db import get_db
from .. import models, schemas

router = APIRouter(prefix="/corpuses", tags=["corpuses"])

@router.post("", response_model=schemas.CorpusOut)
def create_corpus(payload: schemas.CorpusCreate, db: Session = Depends(get_db)):
    city = db.get(models.City, payload.city_id)
    if not city:
        raise HTTPException(status_code=400, detail="City not found")
    dup = db.scalar(select(models.Corpus).where(
        and_(models.Corpus.city_id == payload.city_id, models.Corpus.name == payload.name)
    ))
    if dup:
        raise HTTPException(status_code=409, detail="Corpus with this name already exists in this city")
    obj = models.Corpus(city_id=payload.city_id, name=payload.name)
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@router.get("", response_model=list[schemas.CorpusOut])
def list_corpuses(
    city_id: int | None = None,
    skip: int = 0,
    limit: int = Query(100, le=500),
    db: Session = Depends(get_db)
):
    stmt = select(models.Corpus)
    if city_id:
        stmt = stmt.where(models.Corpus.city_id == city_id)
    rows = db.scalars(stmt.offset(skip).limit(limit)).all()
    return rows

@router.get("/with-status", response_model=list[schemas.CorpusStatusOut])
def list_corpuses_with_status(
    city_id: Optional[int] = Query(None, description="Фильтр по городу"),
    q: Optional[str] = Query(None, description="Поиск по названию корпуса"),
    # ⚠️ без pattern/regex — просто строки
    sort_by: str = Query("city_name", description="city_name|corpus_name|status|id"),
    sort_dir: str = Query("asc", description="asc|desc"),
    skip: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=10000),  # поднимем потолок, чтобы точно не падало
    db: Session = Depends(get_db),
):
    MR = models.MainRecord
    C = models.Corpus
    CITY = models.City

    total = func.count(MR.id)
    true_cnt = func.sum(case((MR.status.is_(True), 1), else_=0))
    false_cnt = func.sum(case((MR.status.is_(False), 1), else_=0))

    stmt = (
        select(
            CITY.id.label("city_id"),
            CITY.name.label("city_name"),
            C.id.label("corpus_id"),
            C.name.label("corpus_name"),
            total.label("total"),
            true_cnt.label("true_cnt"),
            false_cnt.label("false_cnt"),
        )
        .join(CITY, C.city_id == CITY.id)
        .outerjoin(MR, MR.corpus_id == C.id)
        .group_by(CITY.id, CITY.name, C.id, C.name)
    )

    if city_id is not None:
        stmt = stmt.where(C.city_id == city_id)
    if q:
        stmt = stmt.where(C.name.ilike(f"%{q.strip()}%"))

    # Санитайз сортировки — любые неожиданные значения приведём к дефолту
    sort_by = (sort_by or "city_name").lower()
    sort_dir = (sort_dir or "asc").lower()
    direction = asc if sort_dir == "asc" else desc

    if sort_by == "id":
        stmt = stmt.order_by(direction(C.id))
    elif sort_by == "corpus_name":
        stmt = stmt.order_by(direction(C.name))
    elif sort_by == "city_name":
        stmt = stmt.order_by(direction(CITY.name), C.name.asc())
    else:
        # sort_by == "status" или что-то иное: сортнём по городу/корпусу стабильно
        stmt = stmt.order_by(CITY.name.asc(), C.name.asc())

    rows = db.execute(stmt.offset(skip).limit(limit)).all()

    out: list[schemas.CorpusStatusOut] = []
    for r in rows:
        status = "mixed"
        if (r.total or 0) > 0:
            if (r.true_cnt or 0) == (r.total or 0):
                status = "true"
            elif (r.true_cnt or 0) == 0:
                status = "false"

        out.append(
            schemas.CorpusStatusOut(
                city_id=r.city_id,
                city_name=r.city_name,
                corpus_id=r.corpus_id,
                corpus_name=r.corpus_name,
                status=status, total=int(r.total or 0),
                true_cnt=int(r.true_cnt or 0),
                false_cnt=int(r.false_cnt or 0),
            )
        )
    return out

@router.get("/{corpus_id}", response_model=schemas.CorpusOut)
def get_corpus(corpus_id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Corpus, corpus_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Corpus not found")
    return obj

@router.put("/{corpus_id}", response_model=schemas.CorpusOut)
def update_corpus(corpus_id: int, payload: schemas.CorpusUpdate, db: Session = Depends(get_db)):
    obj = db.get(models.Corpus, corpus_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Corpus not found")

    if payload.city_id:
        if not db.get(models.City, payload.city_id):
            raise HTTPException(status_code=400, detail="City not found")
        obj.city_id = payload.city_id
    if payload.name:
        dup = db.scalar(select(models.Corpus).where(
            and_(models.Corpus.city_id == obj.city_id, models.Corpus.name == payload.name, models.Corpus.id != obj.id)
        ))
        if dup:
            raise HTTPException(status_code=409, detail="Corpus with this name already exists in this city")
        obj.name = payload.name

    db.commit(); db.refresh(obj)
    return obj

@router.delete("/{corpus_id}", status_code=204)
def delete_corpus(corpus_id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Corpus, corpus_id)
    if not obj:
        return
    db.delete(obj); db.commit()