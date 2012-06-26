from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base
from haltestellen import Haltestelle

class Fussweg(Base):
    __tablename__ = 'ivu_fusswege'
    __table_args__ = (  UniqueConstraint('von_haltestelle_id', 'nach_haltestelle_id'), )

    id = Column(Integer, primary_key=True)

    zeit = Column(Time, nullable=False)
    startflag = Column(Boolean)

    von_haltestelle_id = Column(Integer, ForeignKey('ivu_halteste.id'), nullable=False)
    von_haltestelle = relationship("Haltestelle", backref=backref('fusswege_aus'), primaryjoin=von_haltestelle_id==Haltestelle.id)

    nach_haltestelle_id = Column(Integer, ForeignKey('ivu_halteste.id'), nullable=False)
    nach_haltestelle = relationship("Haltestelle", backref=backref('fusswege_ein'), primaryjoin=nach_haltestelle_id==Haltestelle.id)
