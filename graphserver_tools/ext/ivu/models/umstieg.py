from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base

from linien import *
from betriebe import *


class BetriebsUmstieg(Base):
    __tablename__ = 'ivu_umst1'
    __table_args__ = (  UniqueConstraint('von_betrieb_id', 'nach_betrieb_id'), )

    id = Column(Integer, primary_key=True)

    zeit = Column(Time, nullable=False)

    von_betrieb_id = Column(Integer, ForeignKey('ivu_betriebe.id'), nullable=False)
    von_betrieb = relationship("Betrieb", backref=backref('umsteigezeiten_betrieb_aus'), primaryjoin=von_betrieb_id==Betrieb.id)

    nach_betrieb_id = Column(Integer, ForeignKey('ivu_betriebe.id'), nullable=False)
    nach_betrieb = relationship("Betrieb", backref=backref('umsteigezeiten_betrieb_ein'), primaryjoin=nach_betrieb_id==Betrieb.id)


class HaltestelleBetriebUmstieg(Base):
    __tablename__ = 'ivu_umst2'
    __table_args__ = (
                UniqueConstraint('von_betrieb_id', 'nach_betrieb_id', 'haltestelle_id'), )

    id = Column(Integer, primary_key=True)

    zeit = Column(Time, nullable=False)

    haltestelle_id = Column(Integer, ForeignKey('ivu_halteste.id'), nullable=False)
    haltestelle = relationship("Haltestelle", backref=backref('umsteigezeiten_betrieb'))

    von_betrieb_id = Column(Integer, ForeignKey('ivu_betriebe.id'), nullable=False)
    von_betrieb = relationship("Betrieb", backref=backref('umsteigezeiten_haltestelle_betrieb_aus'), primaryjoin=von_betrieb_id==Betrieb.id)

    nach_betrieb_id = Column(Integer, ForeignKey('ivu_betriebe.id'), nullable=False)
    nach_betrieb = relationship("Betrieb", backref=backref('umsteigezeiten_haltestelle_betrieb_ein'), primaryjoin=nach_betrieb_id==Betrieb.id)



umst3_von_association_table = Table('umst3_von', Base.metadata,
    Column('umst3_id', Integer, ForeignKey('ivu_umst3.id')),
    Column('linien_id', Integer, ForeignKey('ivu_linien.id'))
)

umst3_nach_association_table = Table('umst3_nach', Base.metadata,
    Column('umst3_id', Integer, ForeignKey('ivu_umst3.id')),
    Column('linien_id', Integer, ForeignKey('ivu_linien.id'))
)


class HaltestelleLinieUmstieg(Base):
    __tablename__ = 'ivu_umst3'
    #__table_args__ = ( UniqueConstraint('von_linie_id', 'nach_linie_id', 'haltestelle_id'), )

    id = Column(Integer, primary_key=True)

    zeit = Column(Time, nullable=False)
    gesichert = Column(Boolean, nullable=False)

    haltestelle_id = Column(Integer, ForeignKey('ivu_halteste.id'), nullable=False)
    haltestelle = relationship("Haltestelle", backref=backref('umsteigezeiten_linie'))

    von_linien = relationship("Linie", backref=backref('umsteigezeiten_aus'), secondary=umst3_von_association_table)

    nach_linien = relationship("Linie", backref=backref('umsteigezeiten_ein'), secondary=umst3_nach_association_table)


class FahrtUmstieg(Base):
    __tablename__ = 'ivu_umst4'
    __table_args__ = (
                UniqueConstraint('haltestelle_id', 'von_fahrt_id', 'nach_fahrt_id'), )

    id = Column(Integer, primary_key=True)

    zeit = Column(Time, nullable=False)
    gesichert = Column(Boolean, nullable=False)

    haltestelle_id = Column(Integer, ForeignKey('ivu_halteste.id'), nullable=False)
    haltestelle = relationship("Haltestelle", backref=backref('umsteigezeiten_fahrt'))

    von_fahrt_id = Column(Integer, ForeignKey('ivu_fahrten.id'))
    von_fahrt = relationship("Fahrt", backref=backref('umsteigezeiten_aus'), primaryjoin=von_fahrt_id==Fahrt.id)

    nach_fahrt_id = Column(Integer, ForeignKey('ivu_fahrten.id'))
    nach_fahrt = relationship("Fahrt", backref=backref('umsteigezeiten_ein'), primaryjoin=nach_fahrt_id==Fahrt.id)

