import datetime
import os
import psycopg2
import threading
import sys

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session

from graphserver_tools.ext.visumPuTTables import VisumPuTTables
from graphserver_tools.ext.ivu.models import *
from graphserver_tools.ext.ivu.read_ivu import read as readIvuToDb
from graphserver_tools.utils import utils


def removeSpecialCharacter(s):
    return s.replace(';', '_').replace('$', '_')


class IvuToVisum(VisumPuTTables):

    def __init__(self, db_connect_string, date=datetime.datetime(2011,1,1), recreate_tables=False, read_ivu_data=False):
        self.db_connect_string = db_connect_string
        self.date = date
        self.read_ivu_data = read_ivu_data

        self._createDbTables(recreate_tables)
        self._truncateDbTables()


    #
    # setter & getter
    #
    def setIvuData(self, ivu_data):
        self._ivu_data = ivu_data

        self._session = self._getNewSession()

        if self.read_ivu_data:
            readIvuToDb(ivu_data, self.db_connect_string)


    def getIvuData(self):
        return self.self._ivu_data

    ivu_data = property(getIvuData, setIvuData)


    #
    # other public methods
    #
    def transform(self):
        ''' Converts the feed associated with this object into a data for a visum database.
        '''

        print 'creating internal data structures'
        self._getValidUnterlinien()
        self._getDirections()


        print 'converting'
        self._processBetreiber()
        self._processVsysset()
        self._processKnoten()

        threads = []

        hst_t = threading.Thread(target=self._processHstHstBereichHstPunkt)
        hst_t.start()

        for m in (  self._processLinieRouteElement,
                    self._processFahrzeitprofil,
                    self._processFahrzeitprofilelement,
                    self._processFahrplanfahrt,
                    self._processZwischenpunkte
                 ):

            t = threading.Thread(target=m)
            t.start()

            threads.append(t)

        hst_t.join()

        self._processUebergangsGehzeitenHaltestellenbereich()

        for t in threads:
            t.join()


    #
    # private methods
    #
    def _getNewSession(self):
        # this is not nice but won't break any other code
        user = self.db_connect_string.split()[1].split('=')[1]
        pwd = self.db_connect_string.split()[2].split('=')[1]
        host = self.db_connect_string.split()[3].split('=')[1]
        dbname = self.db_connect_string.split()[0].split('=')[1]
        port = self.db_connect_string.split()[4].split('=')[1]

        engine = create_engine('postgresql://'+user+':'+pwd+'@'+host+':'+port+'/'+dbname, echo=False)

        Session = scoped_session(sessionmaker(bind=engine))

        return Session()



    def _getValidUnterlinien(self):
        """
            creates a dictionary mapping between linien and their unterlinien.
            creates a list of all valid unterlinien
        """

        # get all unterlinien
        unterlinien = self._session.query(Linie).all()

        # remove all unterlinen not valid on entered date
        unterlinien = [ ul for ul in unterlinien if ul.isValidOnDate(self._session, self.date) ]


        self.linien_unterlinien_mapper = {}

        for ul in unterlinien:

            if ( ul.liniennummer, ul.betrieb ) in self.linien_unterlinien_mapper:
                self.linien_unterlinien_mapper[( ul.liniennummer, ul.betrieb )].append(ul)

            else:
                self.linien_unterlinien_mapper[( ul.liniennummer, ul.betrieb )] = [ ul, ]


        # compute which versions are valid for each linie
        linien_version_mapper = {}

        for l in self.linien_unterlinien_mapper:

            valid_version = self.linien_unterlinien_mapper[l][0].version
            version_prioritaet = self.linien_unterlinien_mapper[l][0].prioritaet

            for ul in self.linien_unterlinien_mapper[l]:

                if ul.prioritaet > version_prioritaet:
                    valid_version = ul.version
                    version_prioritaet = ul.prioritaet

            linien_version_mapper[l] = valid_version

        self.unterlinien =  [ ul for ul in unterlinien if ul.version == linien_version_mapper[( ul.liniennummer, ul.betrieb )] and ul.isValidOnDate(self._session, self.date)]


    def _getDirections(self):
        """
            creates a dictionary mapping ivu-directions to visum-directions ( '<', '>' )
        """

        self.direction_mapper = {}

        linien_versions = set()

        for ul in self.unterlinien:
            linien_versions.add(( ul.betrieb, ul.liniennummer, ul.version ))

        for lv in linien_versions:
            linien = self._session.query(Linie).filter(and_(Linie.betrieb==lv[0], and_(Linie.liniennummer==lv[1], Linie.version==lv[2]))).all()

            directions = list(set([l.richtungskuerzel for l in linien]))

            self.direction_mapper[directions[0]] = '>'

            if len(directions) == 2:
                self.direction_mapper[directions[1]] = '<'


    def _processKnoten(self):
        ''' Method will write a vertex (Knoten) for every Haltestelle
            in the feed into the visum database.
        '''
        vertices = []
        session = self._getNewSession()

        haltestellen = session.query(Haltestelle).all()

        for h in haltestellen:

                x_koordinate = h.x_koordinate if h.x_koordinate else 0
                y_koordinate = h.y_koordinate if h.y_koordinate else 0

                vertices.append({   'id':h.id,
                                    'xkoord':x_koordinate,
                                    'ykoord':y_koordinate
                                })


        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "KNOTEN" VALUES (%(id)s, %(xkoord)s, %(ykoord)s)''', vertices)

        c.close()
        conn.commit()

        print '\tfinished converting Knoten'


    def _processZwischenpunkte(self):

        session = self._getNewSession()

        strecken_ivu = [ s for s in session.query(Strecke).all() if s.isValidOnDate(session, self.date) ]

        strecken = []
        strecken_poly = []
        visum_strecken_ids = {}

        vsysset = ','.join(set([ v.verkehrsmittelkuerzel for v in session.query(Verkehrsmittel).all()]))

        for s in strecken_ivu:

            # keine doppelten Strecken und Streckenpolygone einfuegen
            if (s.von_haltestelle.id, s.nach_haltestelle.id) not in visum_strecken_ids:
                # wenn Strecke in Gegenrichtung schon vorhanden, nimm diese Nummer,
                if (s.von_haltestelle.id, s.nach_haltestelle.id) not in visum_strecken_ids:
                    # keine doppelten Strecken und Streckenpolygone einfuegen
                    if (s.nach_haltestelle.id, s.von_haltestelle.id) in visum_strecken_ids:
                        visum_strecken_id = visum_strecken_ids[(s.nach_haltestelle.id, s.von_haltestelle.id)]
                    else: # sonst nimm die ivu-Strecken-ID
                        visum_strecken_id = s.id
                        visum_strecken_ids[(s.von_haltestelle.id, s.nach_haltestelle.id)] = s.id


                    visum_strecke = {   'nr':visum_strecken_id,
                                        'von_knoten':s.von_haltestelle.id,
                                        'nach_knoten':s.nach_haltestelle.id,
                                        'name':None,
                                        'typnr':1,
                                        'vsysset':vsysset
                                   }


                    strecken.append(visum_strecke)


                    for zp in s.zwischenpunkte:

                        strecken_poly.append({  'von_knoten':s.von_haltestelle.id,
                                                'nach_knoten':s.nach_haltestelle.id,
                                                'index':zp.laufende_nummer,
                                                'x_koord':zp.x_koordinate,
                                                'y_koord':zp.y_koordinate
                                            })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "STRECKE" VALUES
                            (%(nr)s, %(von_knoten)s, %(nach_knoten)s, %(name)s, %(typnr)s,
                             %(vsysset)s)''', strecken)

        c.executemany('''INSERT INTO "STRECKENPOLY" VALUES
                            (%(von_knoten)s, %(nach_knoten)s, %(index)s, %(x_koord)s,
                             %(y_koord)s )''', strecken_poly)

        c.close()
        conn.commit()

        print '\tfinished converting Strecken'


    def _processHstHstBereichHstPunkt(self):
        self._processHaltestelle()
        self._processHaltestellenbereich()
        self._processHaltepunkt()


    def _processHaltestelle(self):
        ''' Method will write a Haltestelle for each IVU-Haltestelle with no
            referenzhaltestelle into the visum database.
        '''
        session = self._getNewSession()


        haltestellen_ivu = session.query(Haltestelle).filter(Haltestelle.referenzhaltestelle == None).all()
        haltestellen = []

        for h in haltestellen_ivu:

            x_koordinate = h.x_koordinate if h.x_koordinate else 0
            y_koordinate = h.y_koordinate if h.y_koordinate else 0

            haltestellen.append({   'nr': h.id,
                                    'code': removeSpecialCharacter(h.haltestellenkuerzel),
                                    'name': removeSpecialCharacter(h.haltestellenlangname),
                                    'typnr': 1,
                                    'xkoord': x_koordinate,
                                    'ykoord': y_koordinate
                                })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "HALTESTELLE" VALUES
                            (%(nr)s, %(code)s, %(name)s, %(typnr)s, %(xkoord)s, %(ykoord)s)''',
                            haltestellen)

        c.close()
        conn.commit()

        print '\tfinished converting Haltestellen'


    def _processHaltestellenbereich(self):
        ''' Method will write a Haltestellenbereich for each IVU-Haltestelle with
            unterhaltestellen or no referenzhaltestelle.
        '''
        session = self._getNewSession()

        haltestellen_ivu = session.query(Haltestelle).filter(or_(Haltestelle.referenzhaltestelle == None, Haltestelle.unterhaltestellen != None)).all()
        hatestellenbereiche = []

        for h in haltestellen_ivu:

            # find the referenzhaltestelle
            ref_hst = h

            while ref_hst.referenzhaltestelle:
                ref_hst = ref_hst.referenzhaltestelle

            hstnr = ref_hst.id

            x_koordinate = h.x_koordinate if h.x_koordinate else 0
            y_koordinate = h.y_koordinate if h.y_koordinate else 0

            hatestellenbereiche.append({    'nr': h.id,
                                            'hstnr': hstnr,
                                            'code': removeSpecialCharacter(h.haltestellenkuerzel),
                                            'name': removeSpecialCharacter(h.haltestellenlangname),
                                            'knotnr': h.id,
                                            'typnr': 1,
                                            'xkoord': x_koordinate,
                                            'ykoord': y_koordinate
                                        })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "HALTESTELLENBEREICH" VALUES
                            (%(nr)s, %(hstnr)s, %(code)s, %(name)s, %(knotnr)s, %(typnr)s, %(xkoord)s, %(ykoord)s)''',
                        hatestellenbereiche)

        c.close()
        conn.commit()

        print '\tfinished converting Haltestellenbereiche'


    def _processHaltepunkt(self):
        ''' Method will add a Haltepunkt to the visum database for each IVU-Haltestelle.
        '''
        session = self._getNewSession()

        haltestellen_ivu = session.query(Haltestelle).all()
        haltepunkte = []

        vsysset = ','.join(set([ v.verkehrsmittelkuerzel for v in session.query(Verkehrsmittel).all()]))

        for h in haltestellen_ivu:

            hstbernr = h.referenzhaltestelle.id if h.referenzhaltestelle else h.id
            # not working --> vsysset = ','.join(set([ l.linie.verkehrsmittel.verkehrsmittelkuerzel for l in h.linienprofile]))

            haltepunkte.append({    'nr' : h.id,
                                    'hstbernr' : hstbernr,
                                    'code' : removeSpecialCharacter(h.haltestellenkuerzel),
                                    'name' : removeSpecialCharacter(h.haltestellenlangname),
                                    'typnr' : 1,
                                    'vsysset' : vsysset,
                                    'depotfzgkombm' : None,
                                    'gerichtet' : 1,
                                    'knotnr' : h.id,
                                    'vonknotnr' : None,
                                    'strnr' : None,
                                    'relpos' : 0,
                              })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "HALTEPUNKT" VALUES
                            (%(nr)s, %(hstbernr)s, %(code)s, %(name)s, %(typnr)s, %(vsysset)s,
                            %(depotfzgkombm)s, %(gerichtet)s, %(knotnr)s, %(vonknotnr)s, %(strnr)s, %(relpos)s)''',
                        haltepunkte)

        c.close()
        conn.commit()

        print '\tfinished converting Haltepunkte'

    def _processBetreiber(self):

        session = self._getNewSession()
        betreiber = []

        for b in session.query(Betrieb).all():

            betreiber.append({  'nr':b.id,
                                'name':removeSpecialCharacter('-'.join((b.betriebsname, b.betriebsteilname))),
                                'ksatz1':0.0,
                                'ksatz2':0.0,
                                'ksatz3':0.0
                            })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "BETREIBER" VALUES
                            (%(nr)s, %(name)s, %(ksatz1)s, %(ksatz2)s, %(ksatz3)s)''', betreiber)

        c.close()
        conn.commit()

        print '\tfinished converting Betreiber'


    def _processLinieRouteElement(self):
        self._processLinie()
        self._processLinienroute()
        self._processLinienroutenelement()


    def _processLinie(self):
        ''' Writes a Linie for each IVU-Linie (not Unterlinie) in the feed into the
            visum database.
        '''

        linien = []

        for linie, unterlinien in self.linien_unterlinien_mapper.items():

            linien.append({ 'name' : removeSpecialCharacter('-'.join(( linie[1].betriebsteilschluessel, str(linie[0]) ))),
                            'vsyscode' : unterlinien[0].verkehrsmittel.verkehrsmittelkuerzel,
                            'tarifsystemmenge' : None,
                            'betreibernr' : linie[1].id
                          })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "LINIE" VALUES
                            (%(name)s, %(vsyscode)s, %(tarifsystemmenge)s, %(betreibernr)s)''',
                        linien)

        c.close()
        conn.commit()

        print '\tfinished converting Linien'


    def _processLinienroute(self):
        ''' Writes a Linienroute for each different set of stops (trip) into the visum database.
        '''

        linienrouten = []

        for ul in self.unterlinien:

            linienrouten.append({   'linname' : removeSpecialCharacter('-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) ))),
                                    'name' : removeSpecialCharacter('-'.join(( ul.oeffentlicher_linienname, str(ul.id) ))),
                                    'richtungscode' : self.direction_mapper[ul.richtungskuerzel],
                                    'istringlinie' : 0
                                })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "LINIENROUTE" VALUES
                            (%(linname)s, %(name)s, %(richtungscode)s, %(istringlinie)s)''',
                        linienrouten)

        c.close()
        conn.commit()

        print '\tfinished converting Linienrouten'


    def _processLinienroutenelement(self):
        ''' Writes all Linienprofile of all valid Unterlinien to the visum database
        '''
        session = self._getNewSession()

        linienroutenelemente = []
        last_haltestelle_id = None

        for ul in self.unterlinien:

            for lp in session.query(Linienprofil).filter(Linienprofil.linie == ul).all():

                if last_haltestelle_id == lp.haltestelle.id: # skip stops at the same haltestelle
                    print "skipping element: %s" % last_haltestelle_id
                    continue

                linienroutenelemente.append({   'linname' : removeSpecialCharacter('-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) ))),
                                                'linroutename' : removeSpecialCharacter('-'.join(( ul.oeffentlicher_linienname, str(ul.id) ))),
                                                'richtungscode' : self.direction_mapper[ul.richtungskuerzel],
                                                'index' : lp.laufende_nummer,
                                                'istroutenpunkt' : 1,
                                                'knotnr' : lp.haltestelle.id,
                                                'hpunktnr' : lp.haltestelle.id
                                            })

                last_haltestelle_id = lp.haltestelle.id

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "LINIENROUTENELEMENT" VALUES
                            (%(linname)s, %(linroutename)s, %(richtungscode)s, %(index)s,
                             %(istroutenpunkt)s, %(knotnr)s, %(hpunktnr)s)''',
                        linienroutenelemente)

        c.close()
        conn.commit()

        print '\tfinished converting Linienroutenelemente'


    def _processFahrzeitprofil(self):
        ''' Writes a Fahrzeitprofil for each Unterlinie into the visum database.
        '''

        fahrzeitprofile = []

        for ul in self.unterlinien:

            fahrzeitprofile.append({    'linname' : removeSpecialCharacter('-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) ))),
                                        'linroutename' : removeSpecialCharacter('-'.join(( ul.oeffentlicher_linienname, str(ul.id) ))),
                                        'richtungscode' : self.direction_mapper[ul.richtungskuerzel],
                                        'name' : ul.unterliniennummer
                                   })


        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "FAHRZEITPROFIL" VALUES
                            (%(linname)s, %(linroutename)s, %(richtungscode)s, %(name)s)''',
                        fahrzeitprofile)

        c.close()
        conn.commit()

        print '\tfinished converting Fahrzeitprofile'


    def _processFahrzeitprofilelement(self):
        ''' Writes Fahrzeitprofilemente for Linienprofile of all valid Unterlinien into
            the visum database.
        '''
        session = self._getNewSession()
        elements = []

        for ul in self.unterlinien:

            arrival_time_min = 0
            arrival_time_hours = 0

            day = 30

            for lp in session.query(Linienprofil).filter(Linienprofil.linie == ul).order_by(Linienprofil.laufende_nummer).all():

                departure_time_min = arrival_time_min + lp.wartezeit.minute
                departure_time_hours = arrival_time_hours + lp.wartezeit.hour
                departure_day = day

                if departure_time_min > 59:
                    departure_time_min -= 60
                    departure_time_hours += 1

                if departure_time_hours > 23:
                    departure_day += 1
                    departure_time_hours -= 24


                arrival = datetime.datetime(1899, 12, day, arrival_time_hours, arrival_time_min)
                departure = datetime.datetime(1899, 12, departure_day, departure_time_hours, departure_time_min )

                elements.append({   'linname' : removeSpecialCharacter('-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) ))),
                                    'linroutename' : removeSpecialCharacter('-'.join(( ul.oeffentlicher_linienname, str(ul.id) ))),
                                    'richtungscode' : self.direction_mapper[ul.richtungskuerzel],
                                    'fzprofilname' : ul.unterliniennummer,
                                    'index' : lp.laufende_nummer,
                                    'lrelemindex' : lp.laufende_nummer,
                                    'aus' : int(not lp.aussteigeverbot),
                                    'ein' : int(not lp.einsteigeverbot),
                                    'ankunft' : arrival,
                                    'abfahrt' : departure,
                                })

                arrival_time_min = departure_time_min + lp.fahrzeit.minute
                arrival_time_hours = departure_time_hours + lp.fahrzeit.hour


                if arrival_time_min > 59:
                    arrival_time_min -= 60
                    arrival_time_hours += 1

                if arrival_time_hours > 23:
                    day += 1
                    arrival_time_hours -= 24



        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "FAHRZEITPROFILELEMENT" VALUES
                            (%(linname)s, %(linroutename)s, %(richtungscode)s, %(fzprofilname)s,
                             %(index)s, %(lrelemindex)s, %(aus)s, %(ein)s, %(ankunft)s,
                             %(abfahrt)s)''',
                        elements)

        c.close()
        conn.commit()

        print '\tfinished converting Fahrzeitprofilelemente'

    def _processFahrplanfahrt(self):
        ''' Writes a Fahrplanfahrt for each trip inside the feed into the visum database.
            Uses the pre-defined Fahrzeitprofile (fahrzeitprofil_mapper).
        '''
        session = self._getNewSession()
        fahrten = []

        for ul in self.unterlinien:

            for f in session.query(Fahrt).filter(Fahrt.linie == ul).all():

                if f.isValidOnDate(session, self.date):

                    departure_hour = int(f.abfahrt[:2])
                    departure_min = int(f.abfahrt[3:5])
                    departure_sec = int(f.abfahrt[6:]) if len(f.abfahrt) == 8 else 0


                    departure = datetime.datetime(1899, 12, 30, 0, 0, 0)

                    departure += datetime.timedelta(hours=departure_hour, minutes=departure_min, seconds=departure_sec)

                    fahrten.append({    'nr' : f.id,
                                        'name' : removeSpecialCharacter(ul.oeffentlicher_linienname),
                                        'abfahrt' : departure,
                                        'linname' : removeSpecialCharacter('-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) ))),
                                        'linroutename' : removeSpecialCharacter('-'.join(( ul.oeffentlicher_linienname, str(ul.id) ))),
                                        'richtungscode' : self.direction_mapper[ul.richtungskuerzel],
                                        'fzprofilname' : ul.unterliniennummer,
                                        'vonfzpelemindex' : f.start_pos,
                                        'nachfzpelemindex' : f.end_pos
                                  })

                    # add folgefahrten
                    if f.zeitspanne:

                        for i in range(f.anzahl_folgefahrten):

                            departure += datetime.timedelta(hours=f.zeitspanne.hours, minutes=f.zeitspanne.minutes)


                            fahrten.append({    'nr' : f.id,
                                                'name' : removeSpecialCharacter(ul.oeffentlicher_linienname),
                                                'abfahrt' : departure,
                                                'linname' : removeSpecialCharacter('-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) ))),
                                                'linroutename' : removeSpecialCharacter('-'.join(( ul.oeffentlicher_linienname, str(ul.id) ))),
                                                'richtungscode' : self.direction_mapper[ul.richtungskuerzel],
                                                'fzprofilname' : ul.unterliniennummer,
                                                'vonfzpelemindex' : f.start_pos,
                                                'nachfzpelemindex' : f.end_pos
                                          })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "FAHRPLANFAHRT" VALUES
                            (%(nr)s, %(name)s, %(abfahrt)s, %(linname)s, %(linroutename)s,
                             %(richtungscode)s, %(fzprofilname)s, %(vonfzpelemindex)s,
                             %(nachfzpelemindex)s)''',
                        fahrten)

        c.close()
        conn.commit()

        print '\tfinished converting Fahrplanfahrten'


    def _processVsysset(self):
        session = self._getNewSession()
        vsyssets_list = []

        for v in session.query(Verkehrsmittel).all():

            vsyssets_list.append({  'code':v.verkehrsmittelkuerzel,
                                    'name':removeSpecialCharacter(v.verkehrsmittelname),
                                    'type':'OV',
                                    'pkwe':1
                                })

        vsyssets_list.append({  'code':'Fuss',
                                'name':'Fuss',
                                'type':'OVFuss',
                                'pkwe':1
                            })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "VSYS" VALUES
                            (%(code)s, %(name)s, %(type)s, %(pkwe)s)''', vsyssets_list)

        c.close()
        conn.commit()

        print '\tfinished converting Verkehrssysteme'


    def _processUebergangsGehzeitenHaltestellenbereich(self):
        session = self._getNewSession()
        zeiten = {}

        haltestellenbereiche = session.query(Haltestelle).filter(or_(Haltestelle.referenzhaltestelle == None, Haltestelle.unterhaltestellen != None)).all()
        haltestellenbereiche = [h.id for h in haltestellenbereiche]

        skipped = 0

        for f in session.query(Fussweg).all():

            von_hst = f.von_haltestelle.id
            nach_hst = f.nach_haltestelle.id

            if von_hst not in haltestellenbereiche:
                von_hst = f.von_haltestelle.referenzhaltestelle.id

            if nach_hst not in haltestellenbereiche:
                nach_hst = f.nach_haltestelle.referenzhaltestelle.id

            if von_hst == nach_hst:
                continue

            time = f.zeit.hour*3600 + f.zeit.minute*60 + f.zeit.second

            if (von_hst, nach_hst) not in zeiten:
                zeiten[(von_hst, nach_hst)] = time

            else:
                zeiten[(von_hst, nach_hst)] = min(time, zeiten[(von_hst, nach_hst)])


        zeiten_list = []

        for (von_hst, nach_hst), time in zeiten.items():

            zeiten_list.append({    'von_hst' : von_hst,
                                    'nach_hst' : nach_hst,
                                    'vsyscode' : 'Fuß',
                                    'zeit' : time
                              })


        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "UEBERGANGSGEHZEITHSTBER" VALUES
                            (%(von_hst)s, %(nach_hst)s, %(vsyscode)s, %(zeit)s)''', zeiten_list)

        c.close()
        conn.commit()

        print '\tfinished converting Uebergangs-Gehzeiten Haltestellenbereich'



def main():
    from optparse import OptionParser
    usage = """usage: python ivuToVisum.py [options] config_file ivu_folder"""
    parser = OptionParser(usage=usage)

    parser.add_option("-e", "--export-only", action="store_true", help="converts the data in the database into the visum format (NO READING OF IVU DATA)", dest="export_only", default=False)
    parser.add_option("-i", "--import-only", action="store_true", help="imports the ivu data into the database (NO CONVERSION INTO VISUM DATA)", dest="import_only", default=False)

    (options, args) = parser.parse_args()

    if len(args) != 2 or not os.path.exists(args[0]) or not os.path.exists(args[1]) or (options.import_only and options.export_only):
        parser.print_help()
        exit(-1)


    defaults = { 'psql-host':'localhost',
                 'psql-port':'5432',
                 'psql-user':'postgres',
                 'psql-password':'',
                 'psql-database':'graphserver',
                 'date':'2011.01.01' }

    config = utils.read_config(args[0], defaults, True)

    psql_connect_string = 'dbname=%s user=%s password=%s host=%s port=%s' % ( config['psql-database'],
                                                                              config['psql-user'],
                                                                              config['psql-password'],
                                                                              config['psql-host'],
                                                                              config['psql-port']       )

    ivu_folder = args[1]

    read_ivu_data = not options.export_only

    transformer = IvuToVisum(psql_connect_string, recreate_tables=False, read_ivu_data=read_ivu_data)
    transformer.ivu_data = ivu_folder
    transformer.date = datetime.date(int(config['date'][:4]), int(config['date'][5:7]), int(config['date'][8:]))

    if not options.import_only:
        print 'converting data'
        transformer.transform()

    print 'done'


if __name__ == '__main__': main()


