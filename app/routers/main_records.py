from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Body, status
from sqlalchemy.orm import Session
from sqlalchemy import case, select, and_, update, func, asc, desc, text
import json, requests, logging
from sqlalchemy.exc import IntegrityError
from ..db import get_db
from .. import models, schemas
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

router = APIRouter(prefix="/main", tags=["main"])

logger = logging.getLogger(__name__)

EXTERNAL_URL = "http://185.13.20.2:8082/rest_api/v2/Homes/"

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
        id=payload.id,
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

@router.get("/with-names/{record_id}", response_model=schemas.MainWithNamesOut)
def get_record_with_names(
    record_id: int,
    db: Session = Depends(get_db),
):
    stmt = (
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
        .where(models.MainRecord.id == record_id)
        .limit(1)
    )

    row = db.execute(stmt).first()
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Record {record_id} not found",
        )

    return schemas.MainWithNamesOut(**row._mapping)


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

@router.patch("/{record_id}/status", response_model=schemas.MainOut, tags=["main"])
def set_record_status(record_id: int, payload: schemas.StatusUpdateIn = Body(...), db: Session = Depends(get_db)):
    obj = db.get(models.MainRecord, record_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Record not found")
    obj.status = payload.status
    db.commit()
    db.refresh(obj)
    return obj


@router.post("/create-with-names", response_model=schemas.MainWithNamesOut)
def create_with_names(
    payload: schemas.MainCreateByNamesIn = Body(...),
    db: Session = Depends(get_db)
):
    # 1) Город
    city_id = payload.city_id
    if not city_id:
        if not payload.city_name:
            raise HTTPException(status_code=400, detail="Provide city_id or city_name")
        city = db.scalar(select(models.City).where(models.City.name == payload.city_name))
        if not city:
            city = models.City(name=payload.city_name)
            db.add(city)
            db.flush()  # получаем id без полного commit
        city_id = city.id

    # 2) Корпус (привязан к городу)
    corpus_id = payload.corpus_id
    if not corpus_id:
        if not payload.corpus_name:
            raise HTTPException(status_code=400, detail="Provide corpus_id or corpus_name")
        corpus = db.scalar(
            select(models.Corpus).where(
                and_(models.Corpus.city_id == city_id, models.Corpus.name == payload.corpus_name)
            )
        )
        if not corpus:
            corpus = models.Corpus(city_id=city_id, name=payload.corpus_name)
            db.add(corpus)
            db.flush()
        corpus_id = corpus.id
    else:
        corpus = db.get(models.Corpus, corpus_id)
        if not corpus or corpus.city_id != city_id:
            raise HTTPException(status_code=400, detail="Corpus does not belong to the specified city")

    # 3) Проверка уникальности main
    dup = db.scalar(
        select(models.MainRecord).where(
            and_(
                models.MainRecord.corpus_id == corpus_id,
                models.MainRecord.street == payload.street,
                models.MainRecord.house_num.is_(payload.house_num) if payload.house_num is None
                else models.MainRecord.house_num == payload.house_num
            )
        )
    )
    if dup:
        raise HTTPException(status_code=409, detail="Record already exists for this (corpus, street, house_num)")

    # 4) Создание main (c опциональным id)
    obj = models.MainRecord(
        id=payload.id,  # может быть None, тогда SERIAL
        corpus_id=corpus_id,
        street=payload.street,
        house_num=payload.house_num,
        status=True if payload.status is None else payload.status
    )
    db.add(obj)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(status_code=409, detail="ID already exists or unique constraint violated")
    db.refresh(obj)

    # 5) Вернём с названиями
    city_name = db.scalar(select(models.City.name).where(models.City.id == city_id))
    corpus_name = db.scalar(select(models.Corpus.name).where(models.Corpus.id == corpus_id))
    return schemas.MainWithNamesOut(
        id=obj.id,
        city_name=city_name,
        corpus_name=corpus_name,
        street=obj.street,
        house_num=obj.house_num,
        status=obj.status,
    )


@router.post("/import-homes", response_model=schemas.ImportSummary, tags=["main"])
def import_homes(payload: schemas.ImportHomesIn = Body(...), db: Session = Depends(get_db)):
    db.execute(text("SET LOCAL statement_timeout = 30000"))

    # 1) Внешний запрос: собираем фильтр только из переданных полей  # NEW
    filt = {"connect_date__gte": payload.connect_date_gte.isoformat()}
    if payload.city_name:   filt["city"]   = payload.city_name.strip()
    if payload.corpus_name: filt["s_liter"] = payload.corpus_name.strip()

    form = {
        "method1": "objects.filter",
        "arg1": json.dumps(filt, ensure_ascii=False),
        "fields": json.dumps(["id","settlement","city","street","s_number","s_liter","connect_date"], ensure_ascii=False),
    }

    s = requests.Session()
    s.mount("http://",  HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.5,
                                                    status_forcelist=[500,502,503,504])))
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.5,
                                                    status_forcelist=[500,502,503,504])))

    resp = None
    try:
        resp = s.post(EXTERNAL_URL, data=form, timeout=60)  # ↑ было 30 → 60c
        logger.info("external status=%s", resp.status_code)
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError:
            preview = (resp.text or "")[:1000]
            logger.error("External returned non-JSON, preview=%r", preview)
            raise HTTPException(status_code=502, detail="External API returned non-JSON")
    except requests.Timeout:
        logger.error("External API timeout (>60s)")
        raise HTTPException(status_code=504, detail="External API timeout")
    except Exception as e:
        # если resp был, сохраним короткий превью тела в лог
        try:
            logger.exception("External API error: %s; body=%r", e, (resp.text[:1000] if resp else None))
        except Exception:
            logger.exception("External API error: %s; body=<unreadable>", e)
        raise HTTPException(status_code=502, detail="External API error")

    # Гарантированно определяем results
    results = (data.get("result") if isinstance(data, dict) else None) or []
    total_source = len(results)
    imported = updated = unchanged = skipped = 0
    warnings: list[str] = []

    # Кэш соответствий (city_name, corpus_name) -> (city_id, corpus_id)  # NEW
    cache: dict[tuple[str, str], tuple[int, int]] = {}

    def get_or_create_city_corpus(city_name_in: str | None, corpus_name_in: str | None) -> tuple[int, int] | None:
        """Вернёт (city_id, corpus_id) для пары названий; создаст при отсутствии."""
        if not city_name_in or not corpus_name_in:
            return None
        key = (city_name_in, corpus_name_in)
        if key in cache:
            return cache[key]
        # город
        city = db.scalar(select(models.City).where(models.City.name == city_name_in))
        if not city:
            city = models.City(name=city_name_in)
            db.add(city); db.flush()
        # корпус
        corpus = db.scalar(select(models.Corpus).where(
            and_(models.Corpus.city_id == city.id, models.Corpus.name == corpus_name_in)
        ))
        if not corpus:
            corpus = models.Corpus(city_id=city.id, name=corpus_name_in)
            db.add(corpus); db.flush()
        cache[key] = (city.id, corpus.id)
        return cache[key]

    for item in results:
        f = (item or {}).get("fields", {}) or {}
        ext_id = f.get("id") or item.get("pk")
        if ext_id is None:
            skipped += 1; warnings.append("Missing id in source record"); continue

        street = (f.get("street") or "").strip()
        if street == "":
            skipped += 1; warnings.append(f"id={ext_id}: empty street"); continue  # CHECK(street<>'') защитим

        house_num_raw = (f.get("s_number") or "").strip()
        house_num = house_num_raw if house_num_raw != "" else None

        # Определяем названия: из payload (если заданы) ИЛИ из самой записи  # NEW
        city_name = (payload.city_name or f.get("city") or "").strip()
        corpus_name = (payload.corpus_name or f.get("s_liter") or "").strip()
        pair = get_or_create_city_corpus(city_name or None, corpus_name or None)
        if not pair:
            skipped += 1; warnings.append(f"id={ext_id}: no city/corpus in payload or source"); continue
        city_id, corpus_id = pair

        try:
            obj = db.get(models.MainRecord, ext_id)
            if obj:
                changed = False
                if obj.corpus_id != corpus_id:
                    obj.corpus_id = corpus_id; changed = True
                if obj.street != street:
                    obj.street = street; changed = True
                if obj.house_num != house_num:
                    obj.house_num = house_num; changed = True
                if changed: updated += 1
                else: unchanged += 1
            else:
                # Проверка уникального (corpus_id, street, house_num)
                dup = db.scalar(
                    select(models.MainRecord).where(
                        and_(
                            models.MainRecord.corpus_id == corpus_id,
                            models.MainRecord.street == street,
                            models.MainRecord.house_num.is_(None) if house_num is None
                            else models.MainRecord.house_num == house_num
                        )
                    )
                )
                if dup:
                    try:
                        dup.id = int(ext_id)   # «привяжем» внешний id
                        updated += 1
                    except Exception:
                        skipped += 1
                        warnings.append(f"id={ext_id}: duplicate by (corpus,street,house_num), cannot set id")
                else:
                    obj = models.MainRecord(
                        id=int(ext_id),
                        corpus_id=corpus_id,
                        street=street,
                        house_num=house_num,
                        status=True,
                    )
                    db.add(obj); imported += 1
        except IntegrityError:
            db.rollback(); skipped += 1; warnings.append(f"id={ext_id}: integrity error")

    db.commit()
    # Можно вернуть city_id/corpus_id из payload (если были), иначе -1  # NEW
    return schemas.ImportSummary(
        total_source=total_source,
        imported=imported,
        updated=updated,
        unchanged=unchanged,
        skipped=skipped,
        city_id=-1 if not payload.city_name else db.scalar(select(models.City.id).where(models.City.name == payload.city_name.strip())),
        corpus_id=-1 if not payload.corpus_name else db.scalar(select(models.Corpus.id).join(models.City).where(
            models.Corpus.name == payload.corpus_name.strip(),
            models.City.name == payload.city_name.strip() if payload.city_name else True
        )) or -1,
        warnings=warnings[:20],
    )


@router.get("/status-summary", response_model=list[schemas.CorpusStatusSummary])
def corpus_status_summary(
    city_id: int | None = None,
    corpus_ids: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db),
):
    # SELECT corpus_id, COUNT(*), SUM(CASE WHEN status THEN 1 ELSE 0 END)
    stmt = select(
        models.MainRecord.corpus_id.label("corpus_id"),
        func.count().label("total"),
        func.sum(case((models.MainRecord.status.is_(True), 1), else_=0)).label("enabled"),
    )

    if city_id:
        # join через Corpus, чтобы отфильтровать по городу
        stmt = stmt.join(models.Corpus, models.Corpus.id == models.MainRecord.corpus_id)\
                   .where(models.Corpus.city_id == city_id)

    if corpus_ids:
        stmt = stmt.where(models.MainRecord.corpus_id.in_(corpus_ids))

    stmt = stmt.group_by(models.MainRecord.corpus_id)
    rows = db.execute(stmt).all()

    out: list[schemas.CorpusStatusSummary] = []
    for corpus_id, total, enabled in rows:
        status = "mixed"
        if total and enabled == total:
            status = "true"
        elif total and enabled == 0:
            status = "false"
        out.append(schemas.CorpusStatusSummary(
            corpus_id=corpus_id, total=total, enabled=enabled or 0, status=status
        ))
    return out