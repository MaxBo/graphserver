from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base

from kalenderdaten import Set_version

class Set_vehicle_type(Base):
    __tablename__ = 'dino_set_vehicle_type'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    veh_type_nr = Column(Integer)
    veh_type_seats = Column(Integer)
    veh_type_straps = Column(Integer)
    handicap_places = Column(Integer)
    veh_type_text = Column(String(40))
    str_veh_type = Column(String(4))


class Set_depot(Base):
    __tablename__ = 'dino_set_depot'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    depot_nr = Column(Integer)
    depot_text = Column(String(40))
    depot_abbrev = Column(String(5))


class Branch(Base):
    __tablename__ = 'dino_branch'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    branch_nr = Column(Integer)
    str_branch_name = Column(String(6))
    branch_name = Column(String(40))


class Lid_travel_time_type(Base):
    __tablename__ = 'dino_lid_travel_time_type'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    line_nr = Column(Integer)
    str_line_var = Column(String(4))
    line_dir_nr = Column(Integer)
    line_consec_nr = Column(Integer)
    timing_group_nr = Column(Integer)
    tt_rel = Column(Integer)
    stopping_time = Column(Integer)


class Set_trip_purpose(Base):
    __tablename__ = 'dino_set_trip_purpose'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    purpose_nr = Column(Integer)
    purpose_text = Column(String(40))
    str_purpose = Column(String(4))


class Lid_course(Base):
    __tablename__ = 'dino_lid_course'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, ForeignKey(Set_version.version))
    line_nr = Column(Integer)
    str_line_var = Column(String(4))
    line_dir_nr = Column(Integer)
    line_consec_nr = Column(Integer)
    stop_nr = Column(Integer)
    stop_type_nr = Column(Integer)
    stopping_point_nr = Column(Integer)
    stopping_point_type = Column(Integer)
    length = Column(Integer)

    ver = relationship(Set_version, primaryjoin = (Set_version.version==version),\
                            foreign_keys=[Set_version.version])

    def isValidOnDate(self, date):
        return self.ver[0].isValidOnDate(date)


class Rec_lin_ber(Base):
    __tablename__ = 'dino_rec_lin_ber'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    branch_nr = Column(Integer)
    branch_name = Column(String(40))
    line_nr = Column(Integer)
    str_line_var = Column(String(4))
    line_name = Column(String(40))
    line_dir_nr = Column(Integer)
    last_modified = Column(String(40))


class Vehicle_destination_text(Base):
    __tablename__ = 'dino_vehicle_destination_text'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    branche_nr = Column(Integer)
    vdt_no = Column(Integer)
    vdt_text_driver1 = Column(String(160))
    vdt_text_driver2 = Column(String(160))
    vdt_text_front1 = Column(String(160))
    vdt_text_front2 = Column(String(160))
    vdt_text_front3 = Column(String(160))
    vdt_text_front4 = Column(String(160))
    vdt_text_side1 = Column(String(160))
    vdt_text_side2 = Column(String(160))
    vdt_text_side3 = Column(String(160))
    vdt_text_side4 = Column(String(160))
    vdt_long_name = Column(String(160))
    vdt_short_name = Column(String(68))


class Trip_vdt(Base):
    __tablename__ = 'dino_trip_vdt'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    timetable_period = Column(String(3))
    line_nr = Column(Integer)
    str_line_var = Column(String(4))
    line_dir_nr = Column(Integer)
    trip_id = Column(Integer)
    line_consec_nr = Column(Integer)
    stop_nr = Column(Integer)
    stop_type_nr = Column(Integer)
    stopping_point_nr = Column(Integer)
    vdt_no = Column(Integer)
