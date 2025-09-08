from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, and_
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
