from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base


class Rec_trip(Base):
    __tablename__ = 'dino_rec_trip'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    line_nr = Column(Integer)
    str_line_var = Column(String(4))
    line_dir_nr = Column(Integer)
    timing_group_nr = Column(Integer)
    trip_attribute = Column(String(1))
    branch_nr = Column(Integer)
    trip_id = Column(Integer)
    trip_id_printing = Column(Integer)
    departure_time = Column(Integer)
    dep_stop_nr = Column(Integer)
    dep_stop_type_nr = Column(Integer)
    dep_stopping_point_nr = Column(Integer)
    arr_stop_nr = Column(Integer)
    arr_stop_type_nr = Column(Integer)
    arr_stopping_point_nr = Column(Integer)
    veh_type_nr = Column(Integer)
    day_attribute_nr = Column(Integer)
    restriction = Column(String(4))
    notice = Column(String(5))
    notice_2 = Column(String(5))
    notice_3 = Column(String(5))
    notice_4 = Column(String(5))
    notice_5 = Column(String(5))
    purpose_nr = Column(Integer)
    round_trip_ID = Column(Integer)
    train_nr = Column(Integer)


class Trip_stop_time(Base):
    __tablename__ = 'dino_trip_stop_time'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    line_nr = Column(Integer)
    str_line_var = Column(String(4))
    line_dir_nr = Column(Integer)
    trip_id = Column(Integer)
    line_consec_nr = Column(Integer)
    stopping_time = Column(Integer)


class Rec_round_trip(Base):
    __tablename__ = 'dino_rec_round_trip'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    day_type_nr = Column(Integer)
    depot_nr = Column(Integer)
    rt_id = Column(Integer)
    veh_typ_nr = Column(Integer)
    dep_stop_type_nr = Column(Integer)
    dep_stop_nr = Column(Integer)
    dep_stopping_point_nr = Column(Integer)
    begin_of_rt = Column(Integer)
    arr_stop_type_nr = Column(Integer)
    arr_stop_nr = Column(Integer)
    arr_stopping_point_nr = Column(Integer)
    end_of_rt = Column(Integer)


class Notice(Base):
    __tablename__ = 'dino_notice'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    line_nr = Column(Integer)
    notice = Column(String(5))
    notice_text1 = Column(String(60))
    notice_text2 = Column(String(60))
    notice_text3 = Column(String(60))
    notice_text4 = Column(String(60))
    notice_text5 = Column(String(60))


class Service_interdiction(Base):
    __tablename__ = 'dino_service_interdiction'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    line_nr = Column(Integer)
    str_line_var = Column(String(4))
    line_dir_nr = Column(Integer)
    trip_id = Column(Integer)
    line_consec_nr = Column(Integer)
    stop_nr = Column(Integer)
    stop_type_nr = Column(Integer)
    stopping_point_nr = Column(Integer)
    service_interdiction_code = Column(String(1))


class Hinw_str(Base):
    __tablename__ = 'dino_hinw_str'

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
    hinw_str_code = Column(String(3))