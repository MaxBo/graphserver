from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base


class Verkehrsmittel(Base):
    __tablename__ = 'ivu_verkehrsm'

    id = Column(Integer, primary_key=True)

    verkehrsmittelkuerzel = Column(String(8), unique=True, index=True)
    verkehrsmittelgattung = Column(String(8), nullable=False) # TODO: replace with ENUM
    verkehrsmittelname = Column(String(40), nullable=False)


class Linie(Base):
    __tablename__ = 'ivu_linien'
    __table_args__ = (
        UniqueConstraint('liniennummer', 'version_id', 'betrieb_id', 'unterliniennummer',
                            'richtungskuerzel' ), )

    id = Column(Integer, primary_key=True)

    liniennummer = Column(String(8), nullable=False)
    prioritaet = Column(Integer, nullable=False)
    unterliniennummer = Column(Integer, nullable=False)
    richtungskuerzel = Column(String(2), nullable=False)
    oeffentlicher_linienname = Column(String(8), nullable=False)

    version_id = Column(Integer, ForeignKey('ivu_versione.id'), nullable=False)
    version = relationship("Version", backref=backref('linien'))

    betrieb_id = Column(Integer, ForeignKey('ivu_betriebe.id'), nullable=False)
    betrieb = relationship("Betrieb", backref=backref('linien'))

    verkehrsmittel_id = Column(Integer, ForeignKey('ivu_verkehrsm.id'), nullable=False)
    verkehrsmittel = relationship("Verkehrsmittel", backref=backref('linien'))

    bitfeld_id = Column(Integer, ForeignKey('ivu_bitfeld.id'))
    bitfeld = relationship("Bitfeld", backref=backref('linien'))


    def isValidOnDate(self, session, date):

        if self.version.isValidOnDate(date):

            for l in session.query(Linie).filter(and_(Linie.betrieb == self.betrieb, and_(Linie.liniennummer == self.liniennummer, Linie.prioritaet > self.prioritaet))).all():
                if l.isValidOnDate(session, date):
                    return False

            if not self.bitfeld:
                return True
            else:
                return self.bitfeld.isValidOnDate(self.version.anfang, date)

        return False



class Linienprofil(Base):
    __tablename__ = 'ivu_linienprofile'
    __table_args__ = (  UniqueConstraint('laufende_nummer', 'linie_id'), )

    id = Column(Integer, primary_key=True)

    laufende_nummer = Column(Integer, nullable=False)
    kilometrierung = Column(Integer)
    position_ankunft = Column(Integer)
    position_abfahrt = Column(Integer)
    fahrzeit = Column(Time, nullable=False)
    wartezeit = Column(Time, nullable=False)
    einsteigeverbot = Column(Boolean)
    aussteigeverbot = Column(Boolean)
    bedarfshalt = Column(Boolean)

    linie_id = Column(Integer, ForeignKey('ivu_linien.id'))
    linie = relationship("Linie", backref=backref('profile'))

    haltestelle_id = Column(Integer, ForeignKey('ivu_halteste.id'))
    haltestelle = relationship("Haltestelle", backref=backref('linienprofile'))


class Fahrt(Base):
    __tablename__ = 'ivu_fahrten'

    id = Column(Integer, primary_key=True)

    start_pos = Column(Integer, nullable=False)
    end_pos = Column(Integer, nullable=False)
    abfahrt = Column(String(8), nullable=False) # time not possible! up to 48:00 possible
    fahrzeitprofil = Column(Integer, nullable=False)
    externe_fahrtennummer = Column(String(10))
    tageskarten_fahrplanbuch = Column(String(7)) # TODO: should get its own table/class
    anzahl_folgefahrten = Column(Integer, nullable=False)
    zeitspanne = Column(Time, nullable=True)

    verkehrsmittel_id = Column(Integer, ForeignKey('ivu_verkehrsm.id'), nullable=True)
    verkehrsmittel = relationship("Verkehrsmittel", backref=backref('fahrtenprofile'))

    bitfeld_id = Column(Integer, ForeignKey('ivu_bitfeld.id'), nullable=True)
    bitfeld = relationship("Bitfeld")

    linie_id = Column(Integer, ForeignKey('ivu_linien.id'), nullable=False)
    linie = relationship("Linie", backref=backref('fahrten'))

    interne_fahrtennummer = Column(String(10))


    def isValidOnDate(self, session, date):

        if self.linie.isValidOnDate(session, date):

            if not self.bitfeld:
                return True
            else:
                return self.bitfeld.isValidOnDate(self.line.version.anfang, date)

        return False


##class FahrtAtt(Base):
##    __tablename__ = 'ivu_fahrtatt'
##
##    id = Column(Integer, primary_key=True)
##
##    betrieb_id = Column(Integer, ForeignKey('ivu_betriebe.id'), nullable=False)
##    betrieb = relationship("Betrieb", backref=backref('linien'))
##
##    liniennummer = Column(String(8), nullable=False)
##    richtungskuerzel = Column(String(2), nullable=False)
##
##    version_id = Column(Integer, ForeignKey('ivu_versione.id'), nullable=False)
##    version = relationship("Version", backref=backref('linien'))
##
##    interne_fahrtennummer = Column(String(10))
##
##    start_pos = Column(Integer, nullable=False)
##    end_pos = Column(Integer, nullable=False)
##
##    attribut_schluessel = Column(String(10))
##
##    wert = Column(String(255))
##
##    bitfeld_id = Column(Integer, ForeignKey('ivu_bitfeld.id'))
##    bitfeld = relationship("Bitfeld", backref=backref('linien'))
##
##    def isValidOnDate(self, session, date):
##
##        if self.linie.isValidOnDate(session, date):
##
##            if not self.bitfeld:
##                return True
##            else:
##                return self.bitfeld.isValidOnDate(self.line.version.anfang, date)
##
##        return False
