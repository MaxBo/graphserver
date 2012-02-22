from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base


class Set_version(Base):
    __tablename__ = 'dino_set_version'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    version_text = Column(String(79))
    timetable_period = Column(String(4))
    tt_period_name = Column(String(40))
    period_date_from = Column(Date)
    period_date_to = Column(Date)
    net_id = Column(String(3))
    period_priority = Column(Integer)

    def isValidOnDate(self, date):

        if date > self.period_date_from and date < self.period_date_to:
            return True
        else:
            return False


class Set_day_type(Base):
    __tablename__ = 'dino_set_day_type'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    day_type_nr = Column(Integer)
    day_type_text = Column(String(40))
    str_day_type = Column(String(2))


class Set_day_attribute(Base):
    __tablename__ = 'dino_set_day_attribute'
    __table_args__ = (  UniqueConstraint('version', 'day_attribute_nr'), )

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    day_attribute_nr = Column(Integer)
    day_attribute_text = Column(String(40))
    str_day_attribute = Column(String(2))


class Day_type_2_day_attribute(Base):
    __tablename__ = 'dino_day_type_2_day_attribute'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    day_type_nr = Column(Integer)
    day_attribute_nr = Column(Integer)


class Calendar_of_the_company(Base):
    __tablename__ = 'dino_calendar_of_the_company'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    day = Column(Date)
    day_text = Column(String(40))
    day_type_nr = Column(Integer)


class Service_restriction(Base):
    __tablename__ = 'dino_service_restriction'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    restriction = Column(String(4))
    restrict_text1 = Column(String(60))
    restrict_text2 = Column(String(60))
    restrict_text3 = Column(String(60))
    restrict_text4 = Column(String(60))
    restrict_text5 = Column(String(60))
    restriction_days = Column(String(192))
    date_from = Column(Date)
    date_until = Column(Date)