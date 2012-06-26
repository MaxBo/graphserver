from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base


class Set_version(Base):
    __tablename__ = 'dino_set_version'
    __table_args__ = (  UniqueConstraint('version'), )

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    version_text = Column(String(79))
    timetable_period = Column(String(4))
    tt_period_name = Column(String(40))
    period_date_from = Column(Date)
    period_date_to = Column(Date)
    net_id = Column(String(3))
    period_priority = Column(Integer)

    def isValidOnDate(self, session, date):
        return date.date() > self.period_date_from and date.date() < self.period_date_to


class Set_day_type(Base):
    __tablename__ = 'dino_set_day_type'
    __table_args__ = (  UniqueConstraint('version', 'day_type_nr'), )

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    day_type_nr = Column(Integer)
    day_type_text = Column(String(40))
    str_day_type = Column(String(2))


class Day_type_2_day_attribute(Base):
    __tablename__ = 'dino_day_type_2_day_attribute'
    __table_args__ = (  UniqueConstraint('version', 'day_type_nr'), )

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    day_type_nr = Column(Integer)
    day_attribute_nr = Column(Integer)
##    ForeignKeyConstraint(['version', 'day_type_nr'], ['dino_set_day_type.version', 'dino_set_day_type.day_type_nr'])
    ForeignKeyConstraint([version, day_type_nr], [Set_day_type.version, Set_day_type.day_type_nr])

    day_type = relationship(Set_day_type, primaryjoin = (Set_day_type.version==version) & (Set_day_type.day_type_nr==day_type_nr),\
                            foreign_keys=[Set_day_type.version, Set_day_type.day_type_nr])


class Set_day_attribute(Base):
    __tablename__ = 'dino_set_day_attribute'
    __table_args__ = (  UniqueConstraint('version', 'day_attribute_nr'), )

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    day_attribute_nr = Column(Integer)
    day_attribute_text = Column(String(40))
    str_day_attribute = Column(String(2))

#    ForeignKeyConstraint(['version', 'day_attribute_nr'], ['dino_day_type_2_day_attribute.version', 'dino_day_type_2_day_attribute.day_attribute_nr'])
    ForeignKeyConstraint([version, day_attribute_nr], [Day_type_2_day_attribute.version, Day_type_2_day_attribute.day_attribute_nr])
    dt = relationship(Day_type_2_day_attribute, primaryjoin = (Day_type_2_day_attribute.version==version) & (Day_type_2_day_attribute.day_attribute_nr==day_attribute_nr),\
                            foreign_keys=[Day_type_2_day_attribute.version, Day_type_2_day_attribute.day_attribute_nr])


    def isValidOnDate(self, session, date):
        daytype = session.query(Calendar_of_the_company).filter_by(day=date.date()).one()
        return daytype.day_type_nr == self.dt[0].day_type_nr





class Calendar_of_the_company(Base):
    __tablename__ = 'dino_calendar_of_the_company'
    __table_args__ = (  UniqueConstraint('version', 'day'), )

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    day = Column(Date)
    day_text = Column(String(40))
    day_type_nr = Column(Integer)
##    ForeignKeyConstraint(['version', 'day_type_nr'], ['dino_set_day_type.version', 'dino_set_day_type.day_type_nr'])
##    ForeignKeyConstraint([version, day_type_nr], [Set_day_type.version, Set_day_type.day_type_nr])
##
##    day_type = relationship(Set_day_type)


class Service_restriction(Base):
    __tablename__ = 'dino_service_restriction'
    __table_args__ = (  UniqueConstraint('version', 'restriction'), )

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
