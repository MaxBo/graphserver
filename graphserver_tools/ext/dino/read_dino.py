import codecs
import datetime
import sys
import time
import os

from multiprocessing import Process

from sqlalchemy import create_engine
from sqlalchemy import and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.types import Integer as IntegerType
from sqlalchemy.types import String as StringType

from graphserver_tools.ext.dino.models import *

##db_connect_string = 'dbname=visum_import user=dino pwd=ggr host=192.168.198.24 port=5432'
db_connect_string = 'dbname=visum_import user=dino pwd=ggr host=localhost port=5432'

def getSession(db_connect_string=db_connect_string, create_tables=False):

    # this is not nice but won't break any other code
    user = db_connect_string.split()[1].split('=')[1]
    pwd = db_connect_string.split()[2].split('=')[1]
    host = db_connect_string.split()[3].split('=')[1]
    dbname = db_connect_string.split()[0].split('=')[1]
    port = db_connect_string.split()[4].split('=')[1]

    engine = create_engine('postgresql://'+user+':'+pwd+'@'+host+':'+port+'/'+dbname, echo=False)

    if create_tables:
        Base.metadata.drop_all(engine) # Base has been imported from dino.models
        Base.metadata.create_all(engine)

    Session = scoped_session(sessionmaker(bind=engine))

    return Session()





'''
TODO:   'umst4.asc' : read_umstieg4
'''


def fileToTuples(file):
    tuples = []

    for ln, line in enumerate(file):
##        if line[0] == '%':
##            continue

        elements = line.split(';')
        line_values = []


        for e in elements:
            e = e.strip(' ')
            line_values.append(e)

        tuples.append([ ln, line_values ])

    return tuples


def int_or_None(s):
    if not s:
        return None
    else:
        return int(s)

def decode_utf(s):
    return s.decode('latin1')

def read_table(fileName, db_connect_string=db_connect_string, folder=r'D:\GIT\gs\graphserver_tools\graphserver_tools\msp\Eingangsdaten\01 Dino\Fahrplan DVG', create_tables=False):
    session = getSession(db_connect_string, create_tables=False)
    newTableClass = eval(fileName.capitalize())
    file = open('%s\\%s.din' %(folder,fileName))
    lines = fileToTuples(file)
    header = lines[0][1]
    table = Base.metadata.tables['dino_%s' %fileName.lower()]
    integerColumns = [isinstance(table.c[columnName.lower()].type, IntegerType) for columnName in header[:-1]]
    stringColumns = [isinstance(table.c[columnName.lower()].type, StringType) for columnName in header[:-1]]
##    print integerColumns
    for ln, line in lines[1:]:
        arguments = {}
        arguments['id'] = ln
        for col in xrange(len(line)-1):
            if integerColumns[col]:
                value = int_or_None(line[col])
            elif stringColumns[col]:
                value = decode_utf(line[col])
            else:
                value = line[col]
            arguments[header[col].lower()] = value
        newRow = newTableClass(**arguments)
        session.add(newRow)

    session.commit()
    print '		finished reading %s' %fileName


def read(folder, db_connect_string):

    ''' Wrapper methods from multiprocessing '''

    def bitfeldVersionaeWrapper(folder, db_connect_string):

        read_versionen(codecs.open(folder+'/set_version.din', encoding='latin-1'), db_connect_string)
        read_daytype(codecs.open(folder+'/set_day_type.din', encoding='latin-1'), db_connect_string)


    def lieferanHaltesteBetriebeWrapper(folder, db_connect_string):

        read_lieferanten(codecs.open(folder+'/lieferan.asc', encoding='latin-1'), db_connect_string)

        halteste_process = Process(target=read_haltestelle, args=(codecs.open(folder+'/halteste.asc', encoding='latin-1'), db_connect_string))
        halteste_process.start()

        betriebe_process = Process(target=read_betriebe, args=(codecs.open(folder+'/betriebe.asc', encoding='latin-1'), db_connect_string))
        betriebe_process.start()

        halteste_process.join()
        betriebe_process.join()


    def linienWrapper(folder, db_connect_string):

        def readManyLinien(linien, db_connect_string):
            session = getSession(db_connect_string)

            for f in linien:
                read_linien(codecs.open(folder+'/'+f, encoding='latin-1'), session)

        linien_files = []

        for f in os.listdir(folder):
            if f[:2] == 'ld':
                linien_files.append(f)

        num_processes = 8
        num_files_per_process = len(linien_files) / num_processes

        processes = []

        for i in range(num_processes):
            p = Process(target=readManyLinien, args=(linien_files[i*num_files_per_process:(i+1)*num_files_per_process], db_connect_string))
            processes.append(p)

        p = Process(target=readManyLinien, args=(linien_files[(i+1)*num_files_per_process:], db_connect_string))
        processes.append(p)

        for p in processes:
            p.start()

        for p in processes:
            p.join()

        print '		finished reading linien'


    def fahrtenWrapper(folder, db_connect_string):

        def readManyFahrten(fahrten, db_connect_string):
            session = getSession(db_connect_string)

            for f in fahrten:
                read_fahrten(codecs.open(folder+'/'+f, encoding='latin-1'), session)


        fahrten_files = []

        for f in os.listdir(folder):
            if f[:2] == 'fd':
                fahrten_files.append(f)

        num_processes = 8
        num_files_per_process = len(fahrten_files) / num_processes

        processes = []

        for i in range(num_processes):
            p = Process(target=readManyFahrten, args=(fahrten_files[i*num_files_per_process:(i+1)*num_files_per_process], db_connect_string))
            processes.append(p)

        p = Process(target=readManyFahrten, args=(fahrten_files[(i+1)*num_files_per_process:], db_connect_string))
        processes.append(p)

        for p in processes:
            p.start()

        for p in processes:
            p.join()

        print '		finished reading fahrten'


    # setup
    zwischenpunkte_process = None
    fusswege_process = None
    umst1_process = None
    umst2_process = None
    umst3_process = None
    umst4_process = None

    # create tables
    session = getSession(db_connect_string, create_tables=True)
    session.commit()


    print 'started reading: Bitfelder, Verionen, Koordinatensystem, Lieferanten, Haltestellen, Betriebe, Verkehrsmittel'

    bitfeld_versionae_process = Process(target=bitfeldVersionaeWrapper, args=(folder, db_connect_string))
    bitfeld_versionae_process.start()

    koordsys_process = Process(target=read_koordinatensystem, args=(codecs.open(os.path.join(folder, 'koordsys.asc'), encoding='latin-1'), db_connect_string))
    koordsys_process.start()

    lieferan_halteste_betriebe_process = Process(target=lieferanHaltesteBetriebeWrapper, args=(folder, db_connect_string))
    lieferan_halteste_betriebe_process.start()

    verkehrsm_process = Process(target=read_verkehrsmittel, args=(codecs.open(os.path.join(folder, 'verkehrm.asc'), encoding='latin-1'), db_connect_string))
    verkehrsm_process.start()


    bitfeld_versionae_process.join()
    lieferan_halteste_betriebe_process.join()
    verkehrsm_process.join()


    print 'started reading: Linien, Strecken, Fusswege, Umsteigezeiten1, Umsteigezeiten2'

    linien_process = Process(target=linienWrapper, args=(folder, db_connect_string))
    linien_process.start()

    if os.path.exists(os.path.join(folder, 'strecken.asc')):
        zwischenpunkte_process = Process(target=read_zwischenpunkte, args=(codecs.open(os.path.join(folder, 'strecken.asc'), encoding='latin-1'), db_connect_string))
        zwischenpunkte_process.start()

    if os.path.exists(os.path.join(folder, 'fussweg.asc')):
        fusswege_process = Process(target=read_fusswege, args=(codecs.open(os.path.join(folder, 'fussweg.asc'), encoding='latin-1'), db_connect_string))
        fusswege_process.start()

    #if os.path.exists(os.path.join(folder, 'umst1.asc')):
    #    umst1_process = Process(target=read_umst1, args=(codecs.open(os.path.join(folder, 'umst1.asc'), encoding='latin-1'), db_connect_string))
    #    umst1_process.start()

    #if os.path.exists(os.path.join(folder, 'umst2.asc')):
    #    umst2_process = Process(target=read_umst2, args=(codecs.open(os.path.join(folder, 'umst2.asc'), encoding='latin-1'), db_connect_string))
    #    umst2_process.start()


    linien_process.join()

    print 'started reading: Fahrten, Umsteigezeiten3'

    fahrten_process = Process(target=fahrtenWrapper, args=(folder, db_connect_string))
    fahrten_process.start()

    #if os.path.exists(os.path.join(folder, 'umst3.asc')):
    #    umst3_process = Process(target=read_umst3, args=(codecs.open(os.path.join(folder, 'umst3.asc'), encoding='latin-1'), db_connect_string))
    #    umst3_process.start()


    fahrten_process.join()


    #print 'started reading: Umsteigezeiten4'

    #if os.path.exists(os.path.join(folder, 'umst4.asc')):
    #    umst4_process = Process(target=read_umst4, args=(codecs.open(os.path.join(folder, 'umst4.asc'), encoding='latin-1'), db_connect_string))
    #    umst4_process.start()


    if zwischenpunkte_process:
        zwischenpunkte_process.join()

    koordsys_process.join()

    if fusswege_process:
        fusswege_process.join()

    if umst1_process:
        umst1_process.join()

    if umst2_process:
        umst2_process.join()

    if umst3_process:
        umst3_process.join()

    if umst4_process:
        umst4_process.join()


    print '\nfinished reading IVU data'


