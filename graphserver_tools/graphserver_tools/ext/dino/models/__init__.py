from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from anschlussdaten import *
from fahrplandaten import *
from kalenderdaten import *
from linien_netz_betriebsdaten import *
from ortsdaten import *