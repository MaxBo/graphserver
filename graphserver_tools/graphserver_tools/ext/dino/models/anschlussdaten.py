from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.schema import UniqueConstraint

from __init__ import Base


class Rec_connection(Base):
    __tablename__ = 'dino_rec_connection'

    id = Column(Integer, primary_key=True, nullable=False)

    version = Column(Integer)
    orig_day_attribute_nr = Column(Integer)
    orig_line_nr = Column(Integer)
    orig_line_dir_nr = Column(Integer)
    orig_stop_nr = Column(Integer)
    orig_stop_type_nr = Column(Integer)
    orig_stop_area_nr = Column(Integer)
    orig_time_interval_begin = Column(Integer)
    orig_time_interval_end = Column(Integer)
    dest_day_attribute_nr = Column(Integer)
    dest_line_nr = Column(Integer)
    dest_line_dir_nr = Column(Integer)
    dest_stop_nr = Column(Integer)
    dest_stop_type_nr = Column(Integer)
    dest_stop_area_nr = Column(Integer)
    dest_time_interval_begin = Column(Integer)
    dest_time_interval_end = Column(Integer)
    vehicle_cange = Column(Integer)
    transfer_time = Column(Integer)
    transfer_distance = Column(Integer)