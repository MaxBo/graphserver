from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base


class Haltestelle(Base):
    __tablename__ = 'ivu_halteste'
    __table_args__ = (  UniqueConstraint('haltestellennummer', 'lieferant_id'), )

    id = Column(Integer, primary_key=True)

    haltestellennummer = Column(Integer, nullable=False, index=True)
    haltestellentyp = Column(String(2))
    haltestellenkuerzel = Column(String(8))
    x_koordinate = Column(Integer)
    y_koordinate = Column(Integer)
    gemeindeziffer = Column(String(11))
    behindertengerecht = Column(Boolean)
    haltestellenlangname = Column(String(60), nullable=True)
    zielbeschilderung = Column(String(60))
    auskunftsname = Column(String(60))
    satzname = Column(String(60))
    kminfowert = Column(Integer)
    bfpriowert = Column(Integer)
    aliasname = Column(String(60))

    lieferant_id = Column(Integer, ForeignKey('liferan.id'), nullable=False)
    lieferant = relationship("Lieferant", backref=backref('haltestellen'))

Haltestelle.referenzhaltestelle_id = Column(Integer, ForeignKey('halteste.id'))
Haltestelle.referenzhaltestelle = relationship('Haltestelle', backref=backref('unterhaltestellen'), remote_side=Haltestelle.id)








