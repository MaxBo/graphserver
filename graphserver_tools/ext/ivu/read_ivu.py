import codecs
import datetime
import time
import thread
import os

from sqlalchemy import create_engine
from sqlalchemy import and_

from graphserver_tools.ext.ivu.models import *


'''
TODO:   'umst4.asc' : read_umstieg4
'''


# bool values indicating the start of corresponding files
reading_koorsys = False
reading_lieferan = False
reading_bitfeld = False
reading_verkehrsm = False
reading_halteste = False
reading_betreiber = False
reading_versione = False
reading_fusswege = False
reading_zwischenpunkte = False
reading_linien = False
reading_fahrten = False
reading_umst1 = False
reading_umst2 = False
reading_umst3 = False
reading_umst4 = False

# bool values indicating the finish of reading of corresponding files
finished_koorsys = False
finished_lieferan = False
finished_bitfeld = False
finished_verkehrsm = False
finished_halteste = False
finished_betreiber = False
finished_versione = False
finished_fusswege = False
finished_zwischenpunkte = False
finished_linien = False
finished_fahrten = False
finished_umst1 = False
finished_umst2 = False
finished_umst3 = False
finished_umst4 = False




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


def read_lieferanten(lieferanten_file, session):

    for ln, line in fileToTuples(lieferanten_file):
        lieferant = Lieferant(  lieferantenkuerzel=line[0],
                                lieferntenname=line[1]
                             )
        session.add(lieferant)

    session.commit()


def read_verkehrsmittel(verkehrsmittel_file, session):

    for ln, line in fileToTuples(verkehrsmittel_file):
        v_mittel = Verkehrsmittel(  verkehrsmittelkuerzel=line[0],
                                    verkehrsmittelgattung=line[1],
                                    verkehrsmittelname=line[2]
                                 )
        session.add(v_mittel)

    session.commit()


def read_betriebe(betriebe_file, session):

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


def read_bitfeld(bitfeld_file, session):

    for ln, line in fileToTuples(bitfeld_file):

        bitfeld = Bitfeld(  bitfeldnummer=int_or_None(line[0]),
                            bitfeld=line[1]
                         )

        session.add(bitfeld)

    session.commit()


def read_haltestelle(haltestellen_file, session):
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


def read_koordinatensystem(koordinatensystem_file, session):

    ln, line = fileToTuples(koordinatensystem_file)[0]

    koord = Koordinatensystem(  koordinatennummer=int_or_None(line[0]),
                                name=line[1]
                             )

    session.add(koord)
    session.commit()


def read_versionen(versionen_file, session):

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


def read_fusswege(fusswege_file, session):

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


def read_zwischenpunkte(zwischenpunkte_file, session):
    strecke = None

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

            else:

                zwischenpunkt = Zwischenpunkt(  laufende_nummer=int_or_None(line[0]),
                                                x_koordinate=int_or_None(line[1]),
                                                y_koordinate=int_or_None(line[2]),
                                                strecke=strecke
                                             )

                session.add(zwischenpunkt)
        except:
            print "ERROR while processing 'Strecken' in line %s" % (ln +1)
            raise

    session.commit()


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
            for x in range(int_or_None(line[6])):
                ln, line = i.next()

                if (not line[6]) and (not line[7]): # don't read the last haltestelle (it's strange)
                    continue

                haltestelle = session.query(Haltestelle).filter(and_(Haltestelle.lieferant == linie.betrieb.lieferant, Haltestelle.haltestellennummer == line[2])).one()

                if not line[8]: line[8] = False
                if not line[9]: line[9] = False
                if not line[10]: line[10] = False

                if not line[7]: line[7] = '00:00'

                profil = Linienprofil(  laufende_nummer=int_or_None(line[0]),
                                        kilometrierung=int_or_None(line[3]),
                                        position_ankunft=line[4],
                                        position_abfahrt=line[5],
                                        fahrzeit=time_maker(line[6]),
                                        wartezeit=time_maker(line[7]),
                                        einsteigeverbot=line[8],
                                        aussteigeverbot=line[9],
                                        bedarfshalt=line[10],
                                        linie=linie,
                                        haltestelle=haltestelle
                                     )

                session.add(profil)

    except StopIteration:
        pass

    except Exception:
        print 'error in lin %i' % ln
        raise

    session.commit()


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
                verkehrsmittel = session.query(Verkehrsmittel).filter(Verkehrsmittel.verkehrsmittelkuerzel == line[8]).one()
            except:
                pass

            bitfeld = None
            try: # bitfeld is optional
                bitfeld = session.query(Bitfeld).filter(Bitfeld.bitfeldnummer == line[10]).one()
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


def read_umst1(umst1_file, session):

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


def read_umst2(umst2_file, session):

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
            print 'error in line %i - IGNORING' % (int(ln)+1)


    session.commit()


def read_umst3(umst3_file, session):

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


def read_umst4(umst4_file, session):
    finished_umst4 = True


def read(folder, session):

    def read_linien_wrapper(folder, session):
        for f in os.listdir(folder):
            if f[:2] == 'ld':
                read_linien(codecs.open(folder+'/'+f, encoding='latin-1'), session)

    def read_fahrten_wrapper(folder, session):
        for f in os.listdir(folder):
            if f[:2] == 'fd':
                read_fahrten(codecs.open(folder+'/'+f, encoding='latin-1'), session)

    reading = ( reading_koorsys, reading_lieferan, reading_bitfeld, reading_verkehrsm,
                reading_halteste, reading_betreiber, reading_versione, reading_fusswege,
                reading_zwischenpunkte, reading_linien, reading_fahrten, reading_umst1,
                reading_umst2, reading_umst3, reading_umst4 )

    finished = ( finished_koorsys, finished_lieferan, finished_bitfeld, finished_verkehrsm,
                 finished_halteste, finished_betreiber, finished_versione, finished_fusswege,
                 finished_zwischenpunkte, finished_linien, finished_fahrten, finished_umst1,
                 finished_umst2, finished_umst3, finished_umst4 )


    while False in reading:

        if not reading_bitfeld:
            print 'reading bitfelder'
            reading_bitfeld = True
            thread.start_new_thread(read_bitfeld, (codecs.open(folder+'/bitfeld.asc', encoding='latin-1'), session))

        if not reading_lieferan:
            print 'reading lieferanten'
            reading_lieferan = True
            thread.start_new_thread(read_lieferanten, (codecs.open(folder+'/lieferan.asc', encoding='latin-1'), session))

        if not reading_koorsys:
            print 'reading koordinatensystem'
            reading_koorsys = True
            thread.start_new_thread(read_koordinatensystem, (codecs.open(folder+'/koordsys.asc', encoding='latin-1'), session))

        if not reading_verkehrsm:
            print 'reading verkehrsmittel'
            reading_verkehrsm = True
            thread.start_new_thread(read_verkehrsmittel, (codecs.open(folder+'/verkehrm.asc', encoding='latin-1'), session))

        if (not reading_versione) and finished_bitfeld:
            print 'reading versionen'
            reading_versione = True
            thread.start_new_thread(read_versionen, (codecs.open(folder+'/versione.asc', encoding='latin-1'), session))

        if (not reading_halteste) and finished_lieferan:
            print 'reading haltestellen'
            reading_halteste = True
            thread.start_new_thread(read_haltestelle, (codecs.open(folder+'/halteste.asc', encoding='latin-1'), session))

        if (not reading_betreiber) and finished_lieferan:
            print 'reading betreiber'
            reading_betreiber = True
            thread.start_new_thread(read_betriebe, (codecs.open(folder+'/betriebe.asc', encoding='latin-1'), session))

        if (not reading_fusswege) and finished_halteste:
            reading_fusswege = True

            if os.path.exists(folder+'/fussweg.asc'):
                print 'reading fusswege'
                thread.start_new_thread(read_fusswege, (codecs.open(folder+'/fussweg.asc', encoding='latin-1'), session))
            else:
                finished_fusswege = True

        if (not reading_zwischenpunkte) and finished_halteste and finished_versione:
            reading_zwischenpunkte = True

            if os.path.exists(folder+'/strecken.asc'):
                print 'reading zwischenpunkte'
                thread.start_new_thread(read_zwischenpunkte, (codecs.open(folder+'/strecken.asc', encoding='latin-1'), session))
            else:
                finished_zwischenpunkte = True

        if (not reading_linien) and finished_betreiber and finished_halteste and finished_versione and finished_verkehrsm:
            reading_linien = True
            print 'reading linien'
            thread.start_new_thread(read_linien_wrapper, (folder, session))

        if (not reading_fahrten) and finished_linien:
            reading_fahrten = True
            print 'reading fahrten'
            thread.start_new_thread(read_fahrten_wrapper, (folder, session))

        if (not reading_umst1) and finished_betreiber:
            reading_umst1 = True
            if os.path.exists(folder+'/umst1.asc'):
                print 'reading umsteigezeiten1'
                thread.start_new_thread(read_umst1, (codecs.open(folder+'/umst1.asc', encoding='latin-1'), session))
            else:
                finished_umst1 = True

        if (not reading_umst2) and finished_betreiber and finished_halteste:
            reading_umst2 = True
            if os.path.exists(folder+'/umst2.asc'):
                print 'reading umsteigezeiten2'
                thread.start_new_thread(read_umst2, (codecs.open(folder+'/umst2.asc', encoding='latin-1'), session))
            else:
                finished_umst2 = True

        if (not reading_umst3) and finished_linien:
            reading_umst3 = True
            if os.path.exists(folder+'/umst3.asc'):
                print 'reading umsteigezeiten2'
                thread.start_new_thread(read_umst3, (codecs.open(folder+'/umst3.asc', encoding='latin-1'), session))
            else:
                finished_umst3 = True

        if (not reading_umst4) and finished_fahrten:
            reading_umst4 = True
            if os.path.exists(folder+'/umst4.asc'):
                thread.start_new_thread(read_umst4, (codecs.open(folder+'/umst4.asc', encoding='latin-1'), session))
            else:
                finished_umst4 = True

        time.sleep(1)

    while False in finished:
        time.sleep(1)

    print 'finished importing'
