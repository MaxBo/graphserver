from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base


class Koordinatensystem(Base):
    __tablename__ = 'ivu_koordsys'

    id = Column(Integer, primary_key=True)

    koordinatennummer = Column(Integer)
    name = Column(String(60), nullable=False)
