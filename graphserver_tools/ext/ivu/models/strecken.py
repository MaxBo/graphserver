from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base

from haltestellen import Haltestelle
from kalender import Version

class Strecke(Base):
    __tablename__ = 'ivu_strecken'

    id = Column(Integer, primary_key=True)

    laenge = Column(Integer)

    von_haltestelle_id = Column(Integer, ForeignKey('ivu_halteste.id'), nullable=False)
    von_haltestelle = relationship("Haltestelle", backref=backref('strecken_aus'), primaryjoin=von_haltestelle_id==Haltestelle.id)

    nach_haltestelle_id = Column(Integer, ForeignKey('ivu_halteste.id'), nullable=False)
    nach_haltestelle = relationship("Haltestelle", backref=backref('strecken_ein'), primaryjoin=nach_haltestelle_id==Haltestelle.id)

    version_id = Column(Integer, ForeignKey('ivu_versione.id'))
    version = relationship("Version", backref=backref('strecken'))


    def isValidOnDate(self, date):

        if self.version:
            return self.version.isValidOnDate(date)
        else:
            return True


class Zwischenpunkt(Base):
    __tablename__ = 'ivu_zwischenpunkte'
    __table_args__ = (  UniqueConstraint('laufende_nummer', 'strecke_id'), )

    id = Column(Integer, primary_key=True)

    laufende_nummer = Column(Integer)
    x_koordinate = Column(Integer)
    y_koordinate = Column(Integer)

    strecke_id = Column(Integer, ForeignKey('ivu_strecken.id'), nullable=False)
    strecke = relationship("Strecke", backref=backref('zwischenpunkte'))
