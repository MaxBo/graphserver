from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base


class Rec_stop(Base):
    __tablename__ = 'dino_rec_stop'
##    __table_args__ = (  UniqueConstraint('stop_nr', 'stop_type_nr'), )

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    stop_nr = Column(Integer)
    ref_stop_nr = Column(Integer)
    stop_type_nr = Column(Integer)
    stop_name = Column(String(50))
    ref_stop_name = Column(String(50))
    stop_shortname = Column(String(8))
    stop_pos_x = Column(Integer)
    stop_pos_y = Column(Integer)
    place = Column(String(20))
    occ = Column(Integer)
    fare_zone = Column(Integer)
    fare_zone2 = Column(Integer)
    fare_zone3 = Column(Integer)
    fare_zone4 = Column(Integer)
    fare_zone5 = Column(Integer)
    fare_zone6 = Column(Integer)
    ifopt = Column(String)


class Rec_stop_area(Base):
    __tablename__ = 'dino_rec_stop_area'
##    __table_args__ = (  UniqueConstraint('stop_nr', 'stop_type_nr', 'stop_area_nr'), )

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    stop_nr = Column(Integer)
    stop_area_nr = Column(Integer)
    stop_type_nr = Column(Integer)
    stop_area_name = Column(String(50))
    ifopt = Column(String)
##    rec_stop_area_fkey = ForeignKeyConstraint(['stop_nr', 'stop_type_nr'], ['dino_rec_stop.stop_nr', 'dino_rec_stop.stop_type_nr'])


class Rec_stopping_points(Base):
    __tablename__ = 'dino_rec_stopping_points'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    stop_nr = Column(Integer)
    ref_stop_nr = Column(Integer)
    stop_area_nr = Column(Integer)
    stop_type_nr = Column(Integer)
    stopping_point_nr = Column(Integer)
    stopping_point_pos_x = Column(Integer)
    stopping_point_pos_y = Column(Integer)
    segment_id = Column(Integer)
    segment_dist = Column(Integer)
    stop_rbl_nr = Column(Integer)
    stopping_point_sh_ortsname = Column(String(5))
    purpose_ttb = Column(Boolean)
    purpose_stt = Column(Boolean)
    purpose_jp = Column(Boolean)
    purpose_cbs = Column(Boolean)
    ifopt = Column(String)


class Rec_footpath(Base):
    __tablename__ = 'dino_rec_footpath'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer, index=True)
    orig_stop_nr = Column(Integer)
    orig_stop_type_nr = Column(Integer)
    orig_stop_area_nr = Column(Integer)
    dest_stop_nr = Column(Integer)
    dest_stop_type_nr = Column(Integer)
    dest_stop_area_nr = Column(Integer)
    transfer_time = Column(Integer)
    transfer_distance = Column(Integer)