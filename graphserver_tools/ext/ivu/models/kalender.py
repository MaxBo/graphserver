from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from bitstring import BitArray

from __init__ import Base




class Bitfeld(Base):
    __tablename__ = 'ivu_bitfeld'

    id = Column(Integer, primary_key=True)

    bitfeldnummer = Column(Integer, unique=True, index=True)
    bitfeld = Column(String(255), nullable=False)

    bitfeld_list = None # list containing boolean values ( True, False ) representing the bitfeld


    def isValidOnDate(self, start_date, date):

        if not self.bitfeld_list:
            self.bitfeld_list = BitArray(hex=self.bitfeld)

        delta = date - start_date

        if (delta.days < 0) or (delta.days >= len(self.bitfeld_list)):
            return False

        return self.bitfeld_list[delta.days]


class Version(Base):
    __tablename__ = 'ivu_version'

    id = Column(Integer, primary_key=True)

    versionsnummer = Column(Integer, unique=True, nullable=False, index=True)
    name = Column(String(60), nullable=False)
    anfang = Column(Date, nullable=False)
    ende = Column(Date, nullable=False)

    bitfeld_id = Column(Integer, ForeignKey('ivu_bitfeld.id'))
    bitfeld = relationship("Bitfeld", backref=backref('versionen'))


    def isValidOnDate(self, date):

        if date > self.anfang and date < self.ende:

            if self.bitfeld.isValidOnDate(self.anfang, date):
                return True

        return False
