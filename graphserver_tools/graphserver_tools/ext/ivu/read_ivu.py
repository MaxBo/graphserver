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

from graphserver_tools.ext.ivu.models import *



def getSession(db_connect_string, create_tables=False):

    # this is not nice but won't break any other code
    user = db_connect_string.split()[1].split('=')[1]
    pwd = db_connect_string.split()[2].split('=')[1]
    host = db_connect_string.split()[3].split('=')[1]
    dbname = db_connect_string.split()[0].split('=')[1]
    port = db_connect_string.split()[4].split('=')[1]

    engine = create_engine('postgresql://'+user+':'+pwd+'@'+host+':'+port+'/'+dbname, echo=False)

    if create_tables:
        Base.metadata.drop_all(engine) # Base has been imported from ivu.models
        Base.metadata.create_all(engine)

    Session = scoped_session(sessionmaker(bind=engine))

    return Session()





'''
TODO:   'umst4.asc' : read_umstieg4
'''


def fileToTuples(file):
    tuples = []

    for ln, line in enumerate(file):
        if line[0] == '%':
            continue

        elements = line.split('#')
        line_values = []

        for e in elements:
            while e and e[0] == ' ':
                e = e[1:]
            while e and e[-1] == ' ':
                e = e[:-1]

            line_values.append(e)

        tuples.append([ ln, line_values ])

    return tuples


def int_or_None(s):
    if not s:
        return None
    else:
        return int(s)


def read_lieferanten(lieferanten_file, db_connect_string):
    session = getSession(db_connect_string)

    for ln, line in fileToTuples(lieferanten_file):
        lieferant = Lieferant(  lieferantenkuerzel=line[0],
                                lieferntenname=line[1]
                             )
        session.add(lieferant)

    session.commit()
    print '		finished reading lieferanten'


def read_verkehrsmittel(verkehrsmittel_file, db_connect_string):
    session = getSession(db_connect_string)

    for ln, line in fileToTuples(verkehrsmittel_file):
        v_mittel = Verkehrsmittel(  verkehrsmittelkuerzel=line[0],
                                    verkehrsmittelgattung=line[1],
                                    verkehrsmittelname=line[2]
                                 )
        session.add(v_mittel)

    session.commit()
    print '		finished reading verkehrsmittel'


def read_betriebe(betriebe_file, db_connect_string):
    session = getSession(db_connect_string)

    for ln, line in fileToTuples(betriebe_file):
        betrieb = Betrieb(  betriebsnummer=int_or_None(line[0]),
                            betriebskuerzel=line[1],
                            betriebsname=line[2],
                            betriebsteilnummer=int_or_None(line[3]),
                            betriebsteilkuerzel=line[4],
                            betriebsteilname=line[5],
                            betriebsteilschluessel=line[6],
                            verkehrsmittelgattung=line[7]
                         )

        #betrieb.verkehrsmittel = session.query(Verkehrsmittel).filter(Verkehrsmittel.verkehrsmittelgattung == line[7]).one()
        betrieb.lieferant = session.query(Lieferant).filter(Lieferant.lieferantenkuerzel == line[8]).one()

        session.add(betrieb)

    session.commit()
    print '		finished reading betriebe'


def read_bitfeld(bitfeld_file, db_connect_string):
    session = getSession(db_connect_string)

    for ln, line in fileToTuples(bitfeld_file):

        bitfeld = Bitfeld(  bitfeldnummer=int_or_None(line[0]),
                            bitfeld=line[1]
                         )

        session.add(bitfeld)

    session.commit()
    print '		finished reading bitfelder'


def read_haltestelle(haltestellen_file, db_connect_string):
    session = getSession(db_connect_string)
    haltestellen_failed = [] # contains haltestellen where referenzhaltestelle has not been added to session

    for ln, line in fileToTuples(haltestellen_file):

        # transform booleans
        if not line[9]:
            line[9] = False

        haltestelle = Haltestelle(  haltestellennummer=int_or_None(line[0]),
                                    haltestellentyp=line[4],
                                    haltestellenkuerzel=line[5],
                                    x_koordinate=int_or_None(line[6]),
                                    y_koordinate=int_or_None(line[7]),
                                    gemeindeziffer=line[8],
                                    behindertengerecht=line[9],
                                    haltestellenlangname=line[10],
                                    zielbeschilderung=line[11],
                                    auskunftsname=line[12],
                                    satzname=line[13],
                                    kminfowert=int_or_None(line[14]),
                                    bfpriowert=int_or_None(line[15]),
                                    aliasname=line[16]
                                 )

        haltestelle.lieferant = session.query(Lieferant).filter(Lieferant.lieferantenkuerzel == line[1]).one()

        # referenzhaltestelle
        try:
            if line[2]:
                ref_lieferant = session.query(Lieferant).filter(Lieferant.lieferantenkuerzel == line[3]).one()
                haltestelle.referenzhaltestelle = session.query(Haltestelle).filter(and_(Haltestelle.lieferant == ref_lieferant, Haltestelle.haltestellennummer == line[2])).one()

            session.add(haltestelle)
        except Exception as e:
            haltestelle.temp_referenzhaltestelle = ( line[2], line[3] )
            haltestellen_failed.append(haltestelle)


    while len(haltestellen_failed):
        # get a haltestelle
        try:
            haltestelle = i.next()
        except:
            i = iter(haltestellen_failed)
            haltestelle = i.next()

        # try setting the referenzhaltestelle
        try:
            ref_lieferant = session.query(Lieferant).filter(Lieferant.lieferantenkuerzel == haltestelle.temp_referenzhaltestelle[1]).one()
            haltestelle.referenzhaltestelle = session.query(Haltestelle).filter(
                                            and_(Haltestelle.lieferant == ref_lieferant,
                                            Haltestelle.haltestellennummer == haltestelle.temp_referenzhaltestelle[0])).one()

            session.add(haltestelle)
            haltestellen_failed.remove(haltestelle)
        except:
            pass

    session.commit()
    print '		finished reading haltestellen'


def read_koordinatensystem(koordinatensystem_file, db_connect_string):
    session = getSession(db_connect_string)

    ln, line = fileToTuples(koordinatensystem_file)[0]

    koord = Koordinatensystem(  koordinatennummer=int_or_None(line[0]),
                                name=line[1]
                             )

    session.add(koord)
    session.commit()
    print '		finished reading koordinatensystem'


def read_versionen(versionen_file, db_connect_string):
    session = getSession(db_connect_string)

    def datetime_maker(s):
        return datetime.datetime(int_or_None(s[6:]), int_or_None(s[3:5]), int_or_None(s[:2]))

    for ln, line in fileToTuples(versionen_file):
        version = Version(  versionsnummer=int_or_None(line[0]),
                            name=line[1],
                            anfang=datetime_maker(line[2]),
                            ende=datetime_maker(line[3])
                         )

        version.bitfeld = session.query(Bitfeld).filter(Bitfeld.bitfeldnummer == line[4]).one()

        session.add(version)

    session.commit()
    print '		finished reading versionen'


def read_fusswege(fusswege_file, db_connect_string):
    session = getSession(db_connect_string)

    def time_maker(s):
        return datetime.time(int_or_None(s[3:5]), int_or_None(s[:2]))

    for ln, line in fileToTuples(fusswege_file):

        try:
            if not line[6]:
                line[6] = False

            von_lieferant = session.query(Lieferant).filter(Lieferant.lieferantenkuerzel == line[1]).one()
            nach_lieferant = session.query(Lieferant).filter(Lieferant.lieferantenkuerzel == line[3]).one()

            von_haltestelle = session.query(Haltestelle).filter(and_(Haltestelle.lieferant == von_lieferant, Haltestelle.haltestellennummer == line[0])).one()
            nach_haltestelle = session.query(Haltestelle).filter(and_(Haltestelle.lieferant == nach_lieferant, Haltestelle.haltestellennummer == line[2])).one()

            fussweg = Fussweg(  zeit=time_maker(line[4]),
                                startflag=line[6],
                                von_haltestelle=von_haltestelle,
                                nach_haltestelle=nach_haltestelle
                             )

            session.add(fussweg)

            if line[5]:
                fussweg2 = Fussweg( zeit=time_maker(line[5]),
                                    startflag=line[6],
                                    von_haltestelle=nach_haltestelle,
                                    nach_haltestelle=von_haltestelle
                                  )

                session.add(fussweg2)
        except:
            print "ERROR while processing 'Fusswege' in line %s" % (ln +1)
            raise

    session.commit()
    print '		finished reading fusswege'


def read_zwischenpunkte(zwischenpunkte_file, db_connect_string):
    session = getSession(db_connect_string)
    strecke = None

    i = 0 # correct the 'laufende_nummer'

    for ln, line in fileToTuples(zwischenpunkte_file):
        try:
            if len(line) == 7: # Kopfzeile
                version = None
                try: # version is optional
                    version = session.query(Version).filter(Version.versionsnummer == line[3]).one()
                except:
                    pass

                lieferant = session.query(Lieferant).filter(Lieferant.lieferantenkuerzel == line[0]).one()

                von_haltestelle = session.query(Haltestelle).filter(and_(Haltestelle.lieferant == lieferant, Haltestelle.haltestellennummer == line[1])).one()
                nach_haltestelle = session.query(Haltestelle).filter(and_(Haltestelle.lieferant == lieferant, Haltestelle.haltestellennummer == line[2])).one()

                strecke = Strecke(  laenge=int_or_None(line[5]),
                                    von_haltestelle=von_haltestelle,
                                    nach_haltestelle=nach_haltestelle,
                                    version=version,
                                 )

                session.add(strecke)

                i = 0

            else:

                i += 1

                zwischenpunkt = Zwischenpunkt(  laufende_nummer=i,
                                                x_koordinate=int_or_None(line[1]),
                                                y_koordinate=int_or_None(line[2]),
                                                strecke=strecke
                                             )

                session.add(zwischenpunkt)
        except:
            print "ERROR while processing 'Strecken' in line %s" % (ln +1)
            raise

    session.commit()
    print '		finished reading zwischenpunkte'


def read_linien(linien_file, session):

    def time_maker(s):
        if len(s) == 5: # non standard but used
            return datetime.time(0, int_or_None(s[:2]), int_or_None(s[3:]))

        elif len(s) == 6:
            hours = 0
            minutes = int_or_None(s[:3])
            seconds = int_or_None(s[4:])

            while minutes > 59:
                hours += 1
                minutes -= 60

            return datetime.time(hours, minutes, seconds)


    i = iter(fileToTuples(linien_file))

    try:
        while True:
            # read Kopfzeile
            ln, line = i.next()

            version = session.query(Version).filter(Version.versionsnummer == line[1]).one()
            betrieb = session.query(Betrieb).filter(Betrieb.betriebsteilschluessel == line[3]).one()
            verkehrsmittel = session.query(Verkehrsmittel).filter(Verkehrsmittel.verkehrsmittelkuerzel == line[8]).one()

            bitfeld = None
            try:
                bitfeld = session.query(Bitfeld).filter(Bitfeld.bitfeldnummer == line[10]).one()
            except:
                pass

            linie = Linie(  liniennummer=line[0],
                            prioritaet=int_or_None(line[2]),
                            unterliniennummer=int_or_None(line[4]),
                            richtungskuerzel=line[5],
                            oeffentlicher_linienname=line[9],
                            version=version,
                            betrieb=betrieb,
                            verkehrsmittel=verkehrsmittel,
                            bitfeld=bitfeld
                         )

            session.add(linie)



            # read Datenzeilen
            anzStops = int_or_None(line[6])

            for x in range(anzStops):
                ln, line = i.next()

                laufende_nummer=int_or_None(line[0])

                if laufende_nummer == anzStops: # last stop, nur Ankunft

                    position_abfahrt = '0'
                    fahrzeit = '00:00'
                    wartezeit = '00:00'
                    einsteigeverbot = True
                    aussteigeverbot = False

                else:

                    position_abfahrt=line[5]
                    fahrzeit = time_maker(line[6])

                    if not line[7]: line[7] = '00:00'

                    wartezeit = time_maker(line[7])

                    einsteigeverbot = bool(line[8])

                    if laufende_nummer == 1: # first stop, nur Abfahrt
                        aussteigeverbot = True
                    else:
                        aussteigeverbot = bool(line[9])


                haltestelle = session.query(Haltestelle).filter(and_(Haltestelle.lieferant == linie.betrieb.lieferant, Haltestelle.haltestellennummer == line[2])).one()

                profil = Linienprofil(  laufende_nummer=laufende_nummer,
                                        kilometrierung=int_or_None(line[3]),
                                        position_ankunft=line[4],
                                        position_abfahrt=position_abfahrt,
                                        fahrzeit=fahrzeit,
                                        wartezeit=wartezeit,
                                        einsteigeverbot=einsteigeverbot,
                                        aussteigeverbot=aussteigeverbot,
                                        bedarfshalt=bool(line[10]),
                                        linie=linie,
                                        haltestelle=haltestelle
                                     )

                session.add(profil)

    except StopIteration:
        pass

    except Exception:
        print 'linien - error in lin %i' % ln
        raise

    session.commit()


##def read_fahrtatt(fahrtattfile, session):
##    linie = None
##
##    for ln, line in fileToTuples(fahrten_file):
##
##        betrieb = session.query(Betrieb).filter(Betrieb.betriebsteilschluessel == line[0]).one()
##        linie = line[1]
##        richtungskuerzel = linie[2]
##        version = int_or_None(line[3])
##        interne_fahrtennummer = linie[4]
##        start_pos=int_or_None(line[5])
##        end_pos=int_or_None(line[6])
##        attribut_schluessel = linie[7]
##        wert = linie[8]
##        bitfeld = None
##        try: # bitfeld is optional
##            bitfeld = session.query(Bitfeld).filter(Bitfeld.bitfeldnummer == line[9]).one()
##        except:
##            pass
##
##        fahrtatt = FahrtAtt(  betrieb=betrieb,
##                    linie = linie,
##                    richtungskuerzel = richtungskuerzel,
##                    version = version,
##                    interne_fahrtennummer = interne_fahrtennummer,
##                    start_pos = start_pos,
##                    end_pos = end_pos,
##                    attribut_schluessel = attribut_schluessel,
##                    wert = wert,
##                    bitfeld_id = bitfeld
##                     )
##        session.add(fahrtatt)
##        session.commit()

def read_fahrten(fahrten_file, session):

    def time_maker(s):
        if len(s) == 5: # non standard but used
            return datetime.time(int_or_None(s[:2]), int_or_None(s[3:]))

        elif len(s) == 8:
            return datetime.time(int_or_None(s[:2]), int_or_None(s[3:5]), int_or_None(s[6:]))

    linie = None

    for ln, line in fileToTuples(fahrten_file):

        if len(line) == 7: # Kopfzeile
            version = session.query(Version).filter(Version.versionsnummer == line[1]).one()
            betrieb = session.query(Betrieb).filter(Betrieb.betriebsteilschluessel == line[2]).one()

            linie = session.query(Linie).filter(and_(Linie.liniennummer==line[0], and_(Linie.version==version, and_(Linie.betrieb==betrieb, and_(Linie.unterliniennummer==line[4], Linie.richtungskuerzel==line[3]))))).one()

        else:

            if not line[10]: line[10] = None

            verkehrsmittel = None
            try: # verkehrsmittel is optional
                verkehrsmittel = session.query(Verkehrsmittel).filter(Verkehrsmittel.verkehrsmittelkuerzel == line[6]).one()
            except:
                pass

            bitfeld = None
            try: # bitfeld is optional
                bitfeld = session.query(Bitfeld).filter(Bitfeld.bitfeldnummer == line[12]).one()
            except:
                pass

            fahrt = Fahrt(  start_pos=int_or_None(line[0]),
                            end_pos=int_or_None(line[3]),
                            abfahrt=line[2],
                            fahrzeitprofil=int_or_None(line[7]),
                            externe_fahrtennummer=line[8],
                            tageskarten_fahrplanbuch=line[9],
                            anzahl_folgefahrten=int_or_None(line[10]),
                            zeitspanne=time_maker(line[11]),
                            verkehrsmittel=verkehrsmittel,
                            bitfeld=bitfeld,
                            linie=linie
                         )

            session.add(fahrt)

    session.commit()


def read_umst1(umst1_file, db_connect_string):
    session = getSession(db_connect_string)

    def time_maker(s):
        return datetime.time(int_or_None(s[:2]), int_or_None(s[3:]))

    for ln, line in fileToTuples(umst1_file):

        von_betrieb = session.query(Betrieb).filter(Betrieb.betriebsteilschluessel == line[0]).one()
        nach_betrieb = session.query(Betrieb).filter(Betrieb.betriebsteilschluessel == line[1]).one()


        umst1 = BetriebsUmstieg(    von_betrieb=von_betrieb,
                                    nach_betrieb=nach_betrieb,
                                    zeit=time_maker(line[2])
                               )
        session.add(umst1)

    session.commit()
    print '		finished reading umsteigezeiten1'


def read_umst2(umst2_file, db_connect_string):
    session = getSession(db_connect_string)

    def time_maker(s):
        return datetime.time(int_or_None(s[:2]), int_or_None(s[3:]))


    for ln, line in fileToTuples(umst2_file):

        try:
            lieferant = session.query(Lieferant).filter(Lieferant.lieferantenkuerzel == line[1]).one()
            haltestelle = session.query(Haltestelle).filter(and_(Haltestelle.lieferant == lieferant, Haltestelle.haltestellennummer == line[0])).one()

            von_betrieb = session.query(Betrieb).filter(Betrieb.betriebsteilschluessel == line[2]).one()
            nach_betrieb = session.query(Betrieb).filter(Betrieb.betriebsteilschluessel == line[3]).one()

            umst2 = HaltestelleBetriebUmstieg(  haltestelle=haltestelle,
                                                von_betrieb=von_betrieb,
                                                nach_betrieb=nach_betrieb,
                                                zeit=time_maker(line[4])
                                             )

            session.add(umst2)

        except Exception:
            print 'Umsteigezeiten2 - error in line %i - IGNORING' % (int(ln)+1)


    session.commit()
    print '		finished reading umsteigezeiten2'


def read_umst3(umst3_file, db_connect_string):
    session = getSession(db_connect_string)

    def time_maker(s):
        return datetime.time(int_or_None(s[:2]), int_or_None(s[3:]))

    for ln, line in fileToTuples(umst3_file):

        lieferant = session.query(Lieferant).filter(Lieferant.lieferantenkuerzel == line[1]).one()
        haltestelle = session.query(Haltestelle).filter(and_(Haltestelle.lieferant == lieferant, Haltestelle.haltestellennummer == line[0])).one()

        von_betrieb = session.query(Betrieb).filter(Betrieb.betriebsteilschluessel == line[2]).one()
        von_linien = session.query(Linie).filter(and_(Linie.liniennummer==line[3], and_(Linie.betrieb==von_betrieb, Linie.richtungskuerzel==line[4]))).all()

        nach_betrieb = session.query(Betrieb).filter(Betrieb.betriebsteilschluessel == line[5]).one()
        nach_linien = session.query(Linie).filter(and_(Linie.liniennummer==line[6], and_(Linie.betrieb==nach_betrieb, Linie.richtungskuerzel==line[7]))).all()


        if len(line) == 11: # row 'gesichert' might not be there!
            gesichert = line[9]
        else:
            gesichert = False


        umst3 = HaltestelleLinieUmstieg(    haltestelle=haltestelle,
                                            von_linien=von_linien,
                                            nach_linien=nach_linien,
                                            zeit=time_maker(line[8]),
                                            gesichert=gesichert
                                        )

        session.add(umst3)

    session.commit()
    print '		finished reading umsteigezeiten3'


def read_umst4(umst4_file, db_connect_string):
    print '		finished reading umsteigezeiten4'


def read(folder, db_connect_string):

    ''' Wrapper methods from multiprocessing '''

    def bitfeldVersionaeWrapper(folder, db_connect_string):

        read_bitfeld(codecs.open(folder+'/bitfeld.asc', encoding='latin-1'), db_connect_string)
        read_versionen(codecs.open(folder+'/versione.asc', encoding='latin-1'), db_connect_string)


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
