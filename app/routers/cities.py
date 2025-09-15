from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import delete, func, select
from ..db import get_db
from .. import models, schemas

router = APIRouter(prefix="/cities", tags=["cities"])

@router.post("", response_model=schemas.CityOut)
def create_city(payload: schemas.CityCreate, db: Session = Depends(get_db)):
    exists = db.scalar(select(models.City).where(models.City.name == payload.name))
    if exists:
        raise HTTPException(status_code=409, detail="City with this name already exists")
    obj = models.City(name=payload.name)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

@router.get("", response_model=list[schemas.CityOut])
def list_cities(skip: int = 0, limit: int = Query(100, le=500), db: Session = Depends(get_db)):
    rows = db.scalars(select(models.City).offset(skip).limit(limit)).all()
    return rows

@router.get("/{city_id}", response_model=schemas.CityOut)
def get_city(city_id: int, db: Session = Depends(get_db)):
    obj = db.get(models.City, city_id)
    if not obj:
        raise HTTPException(status_code=404, detail="City not found")
    return obj

@router.get("/{city_id}/usage", response_model=schemas.CityUsage)
def city_usage(city_id: int, db: Session = Depends(get_db)):
    city = db.get(models.City, city_id)
    if not city:
        raise HTTPException(status_code=404, detail="City not found")
    corpuses_cnt = db.scalar(select(func.count()).select_from(models.Corpus).where(models.Corpus.city_id == city_id)) or 0
    mains_cnt = db.scalar(
        select(func.count()).select_from(models.MainRecord)
        .join(models.Corpus, models.Corpus.id == models.MainRecord.corpus_id)
        .where(models.Corpus.city_id == city_id)
    ) or 0
    return schemas.CityUsage(city_id=city.id, city_name=city.name, corpuses=corpuses_cnt, mains=mains_cnt)

@router.put("/{city_id}", response_model=schemas.CityOut)
def update_city(city_id: int, payload: schemas.CityUpdate, db: Session = Depends(get_db)):
    obj = db.get(models.City, city_id)
    if not obj:
        raise HTTPException(status_code=404, detail="City not found")
    if payload.name and payload.name != obj.name:
        duplicate = db.scalar(select(models.City).where(models.City.name == payload.name))
        if duplicate:
            raise HTTPException(status_code=409, detail="City with this name already exists")
        obj.name = payload.name
    db.commit(); db.refresh(obj)
    return obj

@router.delete("/{city_id}", status_code=204)
def delete_city(city_id: int, force: bool = Query(False), db: Session = Depends(get_db)):
    city = db.get(models.City, city_id)
    if not city:
        return
    # Подсчёт зависимостей
    corpuses_ids = db.scalars(select(models.Corpus.id).where(models.Corpus.city_id == city_id)).all()
    has_corpuses = len(corpuses_ids) > 0
    has_mains = False
    if has_corpuses:
        has_mains = (db.scalar(select(func.count()).select_from(models.MainRecord).where(models.MainRecord.corpus_id.in_(corpuses_ids))) or 0) > 0

    if (has_corpuses or has_mains) and not force:
        raise HTTPException(status_code=409, detail="City has dependent corpuses/mains; use force=true to cascade")

    # Каскад вручную (если force)
    if force:
        if corpuses_ids:
            db.execute(
                delete(models.MainRecord)
                .where(models.MainRecord.corpus_id.in_(corpuses_ids))
                .execution_options(synchronize_session=False)
            )
        db.execute(
            delete(models.Corpus)
            .where(models.Corpus.city_id == city_id)
            .execution_options(synchronize_session=False)
        )

    db.delete(city)
    db.commit()