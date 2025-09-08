from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select
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
def delete_city(city_id: int, db: Session = Depends(get_db)):
    obj = db.get(models.City, city_id)
    if not obj:
        return
    db.delete(obj)
    db.commit()
