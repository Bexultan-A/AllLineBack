from sqlalchemy import (
    Boolean, Column, ForeignKey, Integer, String, Text,
    UniqueConstraint, CheckConstraint
)
from sqlalchemy.orm import relationship, Mapped, mapped_column
from .db import Base

class City(Base):
    __tablename__ = "cities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)

    corpuses: Mapped[list["Corpus"]] = relationship("Corpus", back_populates="city", cascade="all,delete")

class Corpus(Base):
    __tablename__ = "corpuses"
    __table_args__ = (
        UniqueConstraint("city_id", "name", name="uq_corpuses_city_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    city_id: Mapped[int] = mapped_column(ForeignKey("cities.id", ondelete="CASCADE"), index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    city: Mapped[City] = relationship("City", back_populates="corpuses")
    main_records: Mapped[list["MainRecord"]] = relationship("MainRecord", back_populates="corpus", cascade="all,delete")

class MainRecord(Base):
    __tablename__ = "main"
    __table_args__ = (
        UniqueConstraint("corpus_id", "street", "house_num", name="uq_main_corpus_street_house"),
        CheckConstraint("trim(street) <> ''", name="chk_main_street_nonempty"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    corpus_id: Mapped[int] = mapped_column(ForeignKey("corpuses.id", ondelete="RESTRICT"), index=True, nullable=False)
    street: Mapped[str] = mapped_column(String(255), nullable=False)
    house_num: Mapped[str | None] = mapped_column(String(64), nullable=True)  # nullable по твоему требованию
    status: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")

    corpus: Mapped[Corpus] = relationship("Corpus", back_populates="main_records")
