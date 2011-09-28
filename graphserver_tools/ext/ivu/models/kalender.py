from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base


def hex_to_bool_list(hex_string, verbose=True):
    b_list = []

    for hex in hex_string:
        if hex == '0': b_list.extend(( False, False, False, False))
        elif hex == '1': b_list.extend(( False, False, False, True))
        elif hex == '2': b_list.extend(( False, False, True, False))
        elif hex == '3': b_list.extend(( False, False, True, True))
        elif hex == '4': b_list.extend(( False, True, False, False))
        elif hex == '5': b_list.extend(( False, True, False, True))
        elif hex == '6': b_list.extend(( False, True, True, False))
        elif hex == '7': b_list.extend(( False, True, True, True))
        elif hex == '8': b_list.extend(( True, False, False, False))
        elif hex == '9': b_list.extend(( True, False, False, True))
        elif hex == 'A' or hex == 'a': b_list.extend(( True, False, True, False))
        elif hex == 'B' or hex == 'b': b_list.extend(( True, False, True, True))
        elif hex == 'C' or hex == 'c': b_list.extend(( True, True, False, False))
        elif hex == 'D' or hex == 'd': b_list.extend(( True, True, False, True))
        elif hex == 'E' or hex == 'e': b_list.extend(( True, True, True, False))
        elif hex == 'F' or hex == 'f': b_list.extend(( True, True, True, True))
        else:
            if verbose:
                b_list.extend(( False, False, False, False))
            else:
                raise Exception('unrecognizable character in hex_string')

    return b_list


class Bitfeld(Base):
    __tablename__ = 'bitfeld'

    id = Column(Integer, primary_key=True)

    bitfeldnummer = Column(Integer, unique=True, index=True)
    bitfeld = Column(String(255), nullable=False)

    bitfeld_list = None # list containing boolean values ( True, False ) representing the bitfeld


    def isValidOnDate(self, start_date, date):

        if not self.bitfeld_list:
            self.bitfeld_list = hex_to_bool_list(self.bitfeld)

        delta = date - start_date

        if (delta.days < 0) or (delta.days >= len(self.bitfeld_list)):
            return False

        return self.bitfeld_list[delta.days]


class Version(Base):
    __tablename__ = 'versione'

    id = Column(Integer, primary_key=True)

    versionsnummer = Column(Integer, unique=True, nullable=False, index=True)
    name = Column(String(60), nullable=False)
    anfang = Column(Date, nullable=False)
    ende = Column(Date, nullable=False)

    bitfeld_id = Column(Integer, ForeignKey('bitfeld.id'))
    bitfeld = relationship("Bitfeld", backref=backref('versionen'))


    def isValidOnDate(self, date):

        if date > self.anfang and date < self.ende:

            if self.bitfeld.isValidOnDate(self.anfang, date):
                return True

        return False
