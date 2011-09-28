from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base


class Lieferant(Base):
    __tablename__ = 'liferan'

    id = Column(Integer, primary_key=True, nullable=False)

    lieferantenkuerzel = Column(String(6), unique=True, nullable=False, index=True)
    lieferntenname = Column(String(32))


class Betrieb(Base):
    __tablename__ = 'betriebe'

    id = Column(Integer, primary_key=True)

    betriebsnummer = Column(Integer)
    betriebskuerzel = Column(String(8))
    betriebsname = Column(String(60), nullable=False)

    betriebsteilnummer = Column(Integer)
    betriebsteilkuerzel = Column(String(8))
    betriebsteilname = Column(String(60), nullable = False)
    betriebsteilschluessel = Column(String(6), unique=True, index=True)
    verkehrsmittelgattung = Column(String(8), nullable=False)

    lieferant_id = Column(Integer, ForeignKey('liferan.id'), nullable=False)
    lieferant = relationship("Lieferant", backref=backref('betriebe'))

    #verkehrsmittel_id = Column(Integer, ForeignKey('verkehrsm.id'), nullable=False)
    #verkehrsmittel = relationship("Verkehrsmittel", backref=backref('betriebe'))
