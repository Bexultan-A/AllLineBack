from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, update, func, asc, desc, text
from ..db import get_db
from .. import models, schemas

router = APIRouter(prefix="/main", tags=["main"])

# CRUD
@router.post("", response_model=schemas.MainOut)
def create_record(payload: schemas.MainCreate, db: Session = Depends(get_db)):
    corpus = db.get(models.Corpus, payload.corpus_id)
    if not corpus:
        raise HTTPException(status_code=400, detail="Corpus not found")
    # Unique check
    dup = db.scalar(
        select(models.MainRecord).where(
            and_(
                models.MainRecord.corpus_id == payload.corpus_id,
                models.MainRecord.street == payload.street,
                models.MainRecord.house_num.is_(payload.house_num) if payload.house_num is None else models.MainRecord.house_num == payload.house_num
            )
        )
    )
    if dup:
        raise HTTPException(status_code=409, detail="Record already exists for this (corpus, street, house_num)")
    obj = models.MainRecord(
        corpus_id=payload.corpus_id,
        street=payload.street,
        house_num=payload.house_num,
        status=payload.status if payload.status is not None else True
    )
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@router.get("", response_model=list[schemas.MainOut])
def list_records(
    corpus_id: int | None = None,
    city_id: int | None = None,
    status: bool | None = None,
    street: str | None = None,
    skip: int = 0,
    limit: int = Query(100, le=500),
    db: Session = Depends(get_db)
):
    stmt = select(models.MainRecord)
    if corpus_id:
        stmt = stmt.where(models.MainRecord.corpus_id == corpus_id)
    if city_id:
        # join through corpus -> filter by city
        stmt = stmt.join(models.MainRecord.corpus).where(models.Corpus.city_id == city_id)
    if status is not None:
        stmt = stmt.where(models.MainRecord.status == status)
    if street:
        stmt = stmt.where(models.MainRecord.street.ilike(f"%{street}%"))
    rows = db.scalars(stmt.offset(skip).limit(limit)).all()
    return rows

@router.get("/with-names", response_model=schemas.PageMainWithNames)
def list_records_with_names(
    city_id: int | None = None,
    corpus_id: int | None = None,
    status: bool | None = None,
    street: str | None = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=200),
    sort_by: str = Query("id", pattern="^(id|city_name|corpus_name|street)$"),
    sort_dir: str = Query("asc", pattern="^(asc|desc)$"),
    db: Session = Depends(get_db),
):
    # Базовый запрос с join'ами
    base = (
        select(
            models.MainRecord.id,
            models.City.name.label("city_name"),
            models.Corpus.name.label("corpus_name"),
            models.MainRecord.street,
            models.MainRecord.house_num,
            models.MainRecord.status,
        )
        .join(models.MainRecord.corpus)   # main -> corpuses
        .join(models.Corpus.city)         # corpuses -> cities
    )

    # Фильтры
    if city_id:
        base = base.where(models.Corpus.city_id == city_id)
    if corpus_id:
        base = base.where(models.Corpus.id == corpus_id)
    if status is not None:
        base = base.where(models.MainRecord.status == status)
    if street:
        base = base.where(models.MainRecord.street.ilike(f"%{street}%"))

    # Сортировка (безопасный маппинг)
    sort_map = {
        "id": models.MainRecord.id,
        "city_name": models.City.name,
        "corpus_name": models.Corpus.name,
        "street": models.MainRecord.street,
    }
    order_col = sort_map.get(sort_by, models.MainRecord.id)
    base = base.order_by(asc(order_col) if sort_dir == "asc" else desc(order_col))

    # Подсчёт total (второй лёгкий запрос)
    count_stmt = (
        select(func.count())
        .select_from(models.MainRecord)
        .join(models.MainRecord.corpus)
        .join(models.Corpus.city)
    )
    if city_id:
        count_stmt = count_stmt.where(models.Corpus.city_id == city_id)
    if corpus_id:
        count_stmt = count_stmt.where(models.Corpus.id == corpus_id)
    if status is not None:
        count_stmt = count_stmt.where(models.MainRecord.status == status)
    if street:
        count_stmt = count_stmt.where(models.MainRecord.street.ilike(f"%{street}%"))
    total = db.scalar(count_stmt) or 0

    rows = db.execute(base.offset(skip).limit(limit)).all()
    items = [schemas.MainWithNamesOut(**r._mapping) for r in rows]
    return {"items": items, "total": total}

@router.get("/{record_id}", response_model=schemas.MainOut)
def get_record(record_id: int, db: Session = Depends(get_db)):
    obj = db.get(models.MainRecord, record_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Record not found")
    return obj

@router.put("/{record_id}", response_model=schemas.MainOut)
def update_record(record_id: int, payload: schemas.MainUpdate, db: Session = Depends(get_db)):
    obj = db.get(models.MainRecord, record_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Record not found")

    if payload.corpus_id:
        if not db.get(models.Corpus, payload.corpus_id):
            raise HTTPException(status_code=400, detail="Corpus not found")
        obj.corpus_id = payload.corpus_id
    if payload.street is not None:
        obj.street = payload.street
    if payload.house_num is not None:
        obj.house_num = payload.house_num
    if payload.status is not None:
        obj.status = payload.status

    db.commit(); db.refresh(obj)
    return obj

@router.delete("/{record_id}", status_code=204)
def delete_record(record_id: int, db: Session = Depends(get_db)):
    obj = db.get(models.MainRecord, record_id)
    if not obj:
        return
    db.delete(obj); db.commit()

# 🔥 BULK: смена статуса по городу и корпусу
@router.patch("/status-bulk", response_model=dict)
def bulk_status_change(
    payload: schemas.BulkStatusIn = Body(...),
    db: Session = Depends(get_db)
):
    # Определяем city_id и corpus_id (по id или по имени)
    city_id = payload.city_id
    corpus_id = payload.corpus_id

    if (not city_id or not corpus_id) and (payload.city_name and payload.corpus_name):
        city = db.scalar(select(models.City).where(models.City.name == payload.city_name))
        if not city:
            raise HTTPException(status_code=404, detail="City not found by name")
        corp = db.scalar(select(models.Corpus).where(
            and_(models.Corpus.city_id == city.id, models.Corpus.name == payload.corpus_name)
        ))
        if not corp:
            raise HTTPException(status_code=404, detail="Corpus not found by name within the city")
        city_id = city.id
        corpus_id = corp.id

    if not city_id or not corpus_id:
        raise HTTPException(status_code=400, detail="Provide (city_id & corpus_id) or (city_name & corpus_name)")

    # Проверки существования по id
    if not db.get(models.City, city_id):
        raise HTTPException(status_code=404, detail="City not found")
    corpus = db.get(models.Corpus, corpus_id)
    if not corpus or corpus.city_id != city_id:
        raise HTTPException(status_code=400, detail="Corpus does not belong to the specified city")

    stmt = (
        update(models.MainRecord)
        .where(models.MainRecord.corpus_id == corpus_id)
        .values(status=payload.status)
        .execution_options(synchronize_session=False)
    )
    result = db.execute(stmt)
    db.commit()

    return {"updated": result.rowcount, "status": payload.status, "city_id": city_id, "corpus_id": corpus_id}
