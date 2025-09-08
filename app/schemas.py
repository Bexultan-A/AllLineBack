from pydantic import BaseModel, Field, conint, constr
from typing import List, Optional

# Cities
class CityBase(BaseModel):
    name: constr(strip_whitespace=True, min_length=1, max_length=255)

class CityCreate(CityBase): pass
class CityUpdate(CityBase): pass

class CityOut(CityBase):
    id: int
    class Config:
        from_attributes = True

# Corpuses
class CorpusBase(BaseModel):
    name: constr(strip_whitespace=True, min_length=1, max_length=255)
    city_id: int

class CorpusCreate(CorpusBase): pass
class CorpusUpdate(BaseModel):
    name: Optional[constr(strip_whitespace=True, min_length=1, max_length=255)] = None
    city_id: Optional[int] = None

class CorpusOut(BaseModel):
    id: int
    name: str
    city_id: int
    class Config:
        from_attributes = True

# Main records
class MainBase(BaseModel):
    corpus_id: int
    street: constr(strip_whitespace=True, min_length=1, max_length=255)
    house_num: Optional[constr(strip_whitespace=True, max_length=64)] = None
    status: Optional[bool] = True

class MainCreate(MainBase): pass

class MainUpdate(BaseModel):
    corpus_id: Optional[int] = None
    street: Optional[constr(strip_whitespace=True, min_length=1, max_length=255)] = None
    house_num: Optional[constr(strip_whitespace=True, max_length=64)] = None
    status: Optional[bool] = None

class MainOut(BaseModel):
    id: int
    corpus_id: int
    street: str
    house_num: Optional[str]
    status: bool
    class Config:
        from_attributes = True

class MainWithNamesOut(BaseModel):
    id: int
    city_name: str
    corpus_name: str
    street: str
    house_num: Optional[str] = None
    status: bool
    class Config:
        from_attributes = True 

class PageMainWithNames(BaseModel):
    items: List[MainWithNamesOut]
    total: int 

# Bulk status operation
class BulkStatusIn(BaseModel):
    status: bool = Field(..., description="Новый статус (true/false)")
    # Можно указать (city_id + corpus_id) ИЛИ (city_name + corpus_name)
    city_id: Optional[int] = None
    corpus_id: Optional[int] = None
    city_name: Optional[str] = None
    corpus_name: Optional[str] = None


class MainCreateByNamesIn(BaseModel):
    id: conint(gt=0) = Field(..., description="ID обязателен (целое > 0)")
    # один из пары (city_id | city_name)
    city_id: Optional[int] = None
    city_name: Optional[constr(strip_whitespace=True, min_length=1, max_length=255)] = None
    # один из пары (corpus_id | corpus_name)
    corpus_id: Optional[int] = None
    corpus_name: Optional[constr(strip_whitespace=True, min_length=1, max_length=255)] = None

    street: constr(strip_whitespace=True, min_length=1, max_length=255)
    house_num: Optional[constr(strip_whitespace=True, max_length=64)] = None
    status: Optional[bool] = True