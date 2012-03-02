#-------------------------------------------------------------------------------
# Name:        dino2visum
# Purpose:
#
# Author:      Max Bohnet, Tobias Ottenweller
#
# Created:     29.02.2012
# Copyright:   (c) GGR Stadtentwicklung und Mobilität
#-------------------------------------------------------------------------------
#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import os
import psycopg2
import threading
import sys

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy import over



from string import letters

from graphserver_tools.ext.visumPuTTables import VisumPuTTables
from graphserver_tools.ext.dino.models import *
from graphserver_tools.ext.dino.read_dino import read as readDinoToDb
from graphserver_tools.utils import utils


def removeSpecialCharacter(s):
    return s.replace(';', '_').replace('$', '_')

##db_connect_string = 'dbname=visum_import user=dino password=ggr host=192.168.198.24 port=5432'
db_connect_string = 'dbname=visum_import user=dino password=ggr host=localhost port=5432'

class DinoToVisum(VisumPuTTables):

    def __init__(self, db_connect_string=db_connect_string, date=datetime.datetime(2012,3,6), recreate_tables=False, read_dino_data=False):
        self.db_connect_string = db_connect_string
        self.date = date
        self.read_dino_data = read_dino_data

        self._createDbTables(recreate_tables, user='dino')
        self._truncateDbTables()


    #
    # setter & getter
    #
    def setDinoData(self, dino_data):
        self._dino_data = dino_data

        self._session = self._getNewSession()

        if self.read_dino_data:
            readdinoToDb(dino_data, self.db_connect_string)


    def getDinoData(self):
        return self.self._dino_data

    dino_data = property(getDinoData, setDinoData)


    #
    # other public methods
    #
    def transform(self):
        ''' Converts the feed associated with this object into a data for a visum database.
        '''

        print 'creating internal data structures'
##        self._getValidUnterlinien()
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
        unterlinien = self._session.query(Lin_ber).all()

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
            creates a dictionary mapping dino-directions to visum-directions ( '<', '>' )
        """

        self.direction_mapper = {1: '>', 2: '<'}
        self.direction_mapperHR = {1: 'H', 2: 'R'}

    def _processKnoten(self):
        ''' Method will write a vertex (Knoten) for every Haltestelle
            in the feed into the visum database.
        '''
        vertices = []
        session = self._getNewSession()

        haltestellen = session.query(Rec_stopping_points).all()

        for h in haltestellen:
                hp_nr = h.stop_nr * 100 + h.stop_area_nr*10 + h.stopping_point_nr
                x_koordinate = h.stopping_point_pos_x if h.stopping_point_pos_x > -1 else 0
                y_koordinate = h.stopping_point_pos_y if h.stopping_point_pos_y > -1 else 0

                vertices.append({   'id':hp_nr,
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

        strecken_dino = [ s for s in session.query(Lid_course).all() if s.isValidOnDate(self.date) ]
        strecken = []
        strecken_poly = []
        visum_strecken_ids = {}
        version = None


        vsysset = ','.join(set([ v.str_veh_type for v in session.query(Set_vehicle_type).all()]))

        for s in strecken_dino:

            # keine doppelten Strecken und Streckenpolygone einfuegen
            if (s.von_haltestelle.id, s.nach_haltestelle.id) not in visum_strecken_ids:
                # wenn Strecke in Gegenrichtung schon vorhanden, nimm diese Nummer,
                if (s.von_haltestelle.id, s.nach_haltestelle.id) not in visum_strecken_ids:
                    # keine doppelten Strecken und Streckenpolygone einfuegen
                    if (s.nach_haltestelle.id, s.von_haltestelle.id) in visum_strecken_ids:
                        visum_strecken_id = visum_strecken_ids[(s.nach_haltestelle.id, s.von_haltestelle.id)]
                    else: # sonst nimm die dino-Strecken-ID
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



        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "STRECKE" VALUES
                            (%(nr)s, %(von_knoten)s, %(nach_knoten)s, %(name)s, %(typnr)s,
                             %(vsysset)s)''', strecken)


        c.close()
        conn.commit()

        print '\tfinished converting Strecken'


    def _processHstHstBereichHstPunkt(self):
        self._processHaltestelle()
        self._processHaltestellenbereich()
        self._processHaltepunkt()


    def _processHaltestelle(self):
        ''' Method will write a Haltestelle for each dino-Haltestelle with no
            referenzhaltestelle into the visum database.
        '''
        session = self._getNewSession()


        haltestellen_dino = session.query(Rec_stop).all()
        haltestellen = []
        haltestellen_nr = {}

        for h in haltestellen_dino:
            if not haltestellen_nr.has_key(h.stop_nr):
                haltestellen_nr[h.stop_nr] = None


                x_koordinate = h.stop_pos_x if h.stop_pos_x > -1 else 0
                y_koordinate = h.stop_pos_y if h.stop_pos_y > -1 else 0

                haltestellen.append({   'nr': h.stop_nr,
                                        'code': removeSpecialCharacter(h.stop_shortname),
                                        'name': removeSpecialCharacter(h.stop_name),
                                        'typnr': 1,
                                        'xkoord': x_koordinate,
                                        'ykoord': y_koordinate
                                    })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "HALTESTELLE" ("NR", "CODE", "NAME", "TYPNR", "XKOORD", "YKOORD") VALUES
                            (%(nr)s, %(code)s, %(name)s, %(typnr)s, %(xkoord)s, %(ykoord)s)''',
                            haltestellen)

        c.close()
        conn.commit()

        print '\tfinished converting Haltestellen'


    def _processHaltestellenbereich(self):
        ''' Method will write a Haltestellenbereich for each dino-Haltestelle with
            unterhaltestellen or no referenzhaltestelle.
        '''
        session = self._getNewSession()

        haltestellenbereiche_dino = session.query(Rec_stop_area).all()
        hatestellenbereiche = []
        hstber_id = {}
        vertices = []

        for h in haltestellenbereiche_dino:

            # find the referenzhaltestelle

            hstbernr = h.stop_nr * 10 + h.stop_area_nr
            if not hstber_id.has_key(hstbernr):
                hstber_id[hstbernr] = None

                knotnr = hstbernr + 10000000

                hatestellenbereiche.append({    'nr': hstbernr,
                                                'hstnr': h.stop_nr,
                                                'code': '',
                                                'name': removeSpecialCharacter(h.stop_area_name),
                                                'knotnr': knotnr,
                                                'typnr': 1
                                            })
                vertices.append({   'id':knotnr
                                })


        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "KNOTEN" ("NR") VALUES (%(id)s)''', vertices)

        c.executemany('''INSERT INTO "HALTESTELLENBEREICH" ("NR", "HSTNR", "CODE", "NAME", "KNOTNR", "TYPNR") VALUES
                            (%(nr)s, %(hstnr)s, %(code)s, %(name)s, %(knotnr)s, %(typnr)s)''',
                        hatestellenbereiche)

        c.execute('''UPDATE "HALTESTELLENBEREICH" AS hb SET "XKOORD"=h."XKOORD", "YKOORD"=h."YKOORD" FROM "HALTESTELLE" AS h WHERE hb."HSTNR" = h."NR";''')
        c.execute('''UPDATE "KNOTEN" AS k SET "XKOORD"=h."XKOORD", "YKOORD"=h."YKOORD" FROM "HALTESTELLENBEREICH" AS hb, "HALTESTELLE" AS h WHERE hb."KNOTNR" = k."NR" AND hb."HSTNR" = h."NR" AND k."XKOORD" IS NULL;''')

        c.close()
        conn.commit()

        print '\tfinished converting Haltestellenbereiche'


    def _processHaltepunkt(self):
        ''' Method will add a Haltepunkt to the visum database for each dino-Haltestelle.
        '''
        session = self._getNewSession()

        haltepunkte_dino = session.query(Rec_stopping_points).all()
        haltepunkte = []


        for h in haltepunkte_dino:


            hstbernr = h.stop_nr * 10 + h.stop_area_nr
            hp_nr = h.stop_nr * 100 + h.stopping_point_nr
            knotnr = hp_nr

            # not working --> vsysset = ','.join(set([ l.linie.verkehrsmittel.verkehrsmittelkuerzel for l in h.linienprofile]))

            haltepunkte.append({    'nr' : hp_nr,
                                    'hstbernr' : hstbernr,
                                    'typnr' : h.stop_type_nr,
                                    'vsysset' : self.default_vsysset,
                                    'depotfzgkombm' : None,
                                    'gerichtet' : 1,
                                    'knotnr' : knotnr,
                                    'vonknotnr' : None,
                                    'strnr' : None,
                                    'relpos' : 0
                              })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "HALTEPUNKT" ("NR",
                                                   "HSTBERNR",
                                                   "TYPNR",
                                                   "VSYSSET",
                                                   "DEPOTFZGKOMBMENGE",
                                                   "GERICHTET",
                                                   "KNOTNR",
                                                   "VONKNOTNR",
                                                   "STRNR",
                                                   "RELPOS")
                                                    VALUES
                            (%(nr)s, %(hstbernr)s, %(typnr)s, %(vsysset)s,
                            %(depotfzgkombm)s, %(gerichtet)s, %(knotnr)s, %(vonknotnr)s, %(strnr)s, %(relpos)s)''',
                        haltepunkte)

        c.close()
        conn.commit()

        print '\tfinished converting Haltepunkte'

    def _processBetreiber(self):

        session = self._getNewSession()
        betreiber = []

        for b in session.query(Branch).all():

            betreiber.append({  'nr':b.branch_nr,
                                'name':removeSpecialCharacter(b.branch_name),
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
        self._processLinienroute()
        self._processLinienroute()
        self._processLinienroutenelement()


    def _processLinie(self):

        session = self._getNewSession()
        linien = []

        for l in session.query(Rec_lin_ber).all():
            linienname = '_'.join([str(l.branch_nr), l.line_name])
            linien.append({  'betreibernr':l.branch_nr,
                                'name':linienname,
                                'vsyscode':'B',
                                'tarifsystemmenge':''
                            })
        # ggf. noch ergänzen Liniennummer
        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()


        c.executemany('''INSERT INTO "LINIE" VALUES
                            (%(name)s, %(vsyscode)s, %(tarifsystemmenge)s, %(betreibernr)s)''',
                        linien)

        c.close()
        conn.commit()

        print '\tfinished converting Linien'


    def _processLinienroutenelement(self):
        ''' Writes a Linienprofile for each different set of stops (trip) into the visum database.
        '''
        session = self._getNewSession()
        linienrouten = []
        linienroutenelemente = []

        linien_dino = session.query(Rec_lin_ber).all()


        for l in linien_dino:
            linienroutennamen = []
            linienname = '_'.join([str(l.branch_nr), l.line_name])
            lre = session.query(Lid_course).filter_by(line_nr=l.line_nr).all()
            for lr in lre:
                richtung = self.direction_mapperHR[lr.line_dir_nr]
                linienroutenname = '_'.join([l.line_name, lr.str_line_var, richtung])

                if linienroutenname not in linienroutennamen:
                    linienroutennamen.append(linienroutenname)

                    linienrouten.append({   'linname' : linienname,
                                            'name' : linienroutenname,
                                            'richtungscode' : self.direction_mapper[lr.line_dir_nr],
                                            'istringlinie' : 0
                                        })
                    LRIndex = 0
                    last_haltestelle_id = None



                hpnr = lr.stop_nr * 100 + lr.stopping_point_nr

                knotnr = hpnr


                linienroutenelemente.append({   'linname' : linienname,
                                                'linroutename' : linienroutenname,
                                                'richtungscode' : self.direction_mapper[lr.line_dir_nr],
                                                'index' : lr.line_consec_nr,
                                                'istroutenpunkt' : 1,
                                                'knotnr' : knotnr,
                                                'hpunktnr' : hpnr
                                            })

                last_haltestelle_id = hpnr



        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "LINIENROUTE" ("LINNAME", "NAME", "RICHTUNGCODE", "ISTRINGLINIE") VALUES
                            (%(linname)s, %(name)s, %(richtungscode)s, %(istringlinie)s)''',
                        linienrouten)

        c.executemany('''INSERT INTO "LINIENROUTENELEMENT" ("LINNAME", "LINROUTENAME", "RICHTUNGCODE", "INDEX", "ISTROUTENPUNKT", "KNOTNR", "HPUNKTNR")
                         VALUES
                            (%(linname)s, %(linroutename)s, %(richtungscode)s, %(index)s,
                             %(istroutenpunkt)s, %(knotnr)s, %(hpunktnr)s)''',
                        linienroutenelemente)

        c.close()
        conn.commit()

        print '\tfinished converting Linienrouten und Linienroutenelemente'





    def _processFahrzeitprofil(self):
        ''' Writes a Fahrzeitprofil for each Unterlinie into the visum database.
        '''
        session = self._getNewSession()
        fahrzeitprofile = []
        elements = []

        linien_dino = session.query(Rec_lin_ber).all()
        for l in linien_dino:
            linienname = '_'.join([str(l.branch_nr), l.line_name])
            lre = session.query(Lid_course).filter_by(line_nr=l.line_nr).all()
            for lr in lre:
                FZPNamen = []
                richtung = self.direction_mapperHR[lr.line_dir_nr]
                linienroutenname = '_'.join([l.line_name, lr.str_line_var, richtung])
                fzp = session.query(Lid_travel_time_type).filter_by(line_nr=l.line_nr).filter_by(str_line_var=lr.str_line_var).all() # Richtung scheint bei den Lid_travel_time_type nicht gesetzt zu sein...
                for f in fzp:
                    if f.timing_group_nr not in FZPNamen:
                        FZPNamen.append(f.timing_group_nr)

                        fahrzeitprofile.append({    'linname' : linienname,
                                                    'linroutename' : linienroutenname,
                                                    'richtungscode' : self.direction_mapper[lr.line_dir_nr],
                                                    'name' : fzp[0].timing_group_nr
                                               })

                        arrival_time_min = 0
                        arrival_time_hours = 0

                        day = 30

                    else:
                        departure_time_min = arrival_time_min + f.stopping_time/60
                        departure_day = day

                        if departure_time_min > 59:
                            departure_time_min -= 60
                            departure_time_hours += 1

                        if departure_time_hours > 23:
                            departure_day += 1
                            departure_time_hours -= 24


                    arrival = datetime.datetime(1899, 12, day, arrival_time_hours, arrival_time_min)
                    departure = datetime.datetime(1899, 12, departure_day, departure_time_hours, departure_time_min )

                    elements.append({   'linname' : linienname,
                                        'linroutename' : linienroutenname,
                                        'richtungscode' : self.direction_mapper[lr.line_dir_nr],
                                        'fzprofilname' : fzp.timing_group_nr,
                                        'index' : f.consec_nr,
                                        'lrelemindex' : f.consec_nr,
                                        'aus' : int(not f.tt_rel==-1),
                                        'ein' : int(not f.tt_rel==-1),
                                        'ankunft' : arrival,
                                        'abfahrt' : departure,
                                    })
                    if f.tt_rel >= 0:
                        arrival_time_min = departure_time_min + f.tt_rel/60


                    if arrival_time_min > 59:
                        arrival_time_min -= 60
                        arrival_time_hours += 1

                    if arrival_time_hours > 23:
                        day += 1
                        arrival_time_hours -= 24





        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "FAHRZEITPROFIL" ("LINNAME", "LINROUTENAME", "RICHTUNGCODE", "NAME") VALUES
                            (%(linname)s, %(linroutename)s, %(richtungscode)s, %(name)s)''',
                        fahrzeitprofile)

        c.executemany('''INSERT INTO "FAHRZEITPROFILELEMENT" ("LINNAME", "LINROUTENAME", "RICHTUNGCODE", "FZPROFILNAME", "INDEX", "LRELEMINDEX", "AUS", "EIN", "ANKUNFT", "ABFAHRT")
                         VALUES
                            (%(linname)s, %(linroutename)s, %(richtungscode)s, %(fzprofilname)s,
                             %(index)s, %(lrelemindex)s, %(aus)s, %(ein)s, %(ankunft)s,
                             %(abfahrt)s)''',
                        elements)

        c.close()
        conn.commit()

        print '\tfinished converting Fahrzeitprofile'


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
                                        'linroutename' : removeSpecialCharacter('-'.join(( ul.oeffentlicher_linienname, str(ul.id) ))).lstrip('-'),
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
                                                'linroutename' : removeSpecialCharacter('-'.join(( ul.oeffentlicher_linienname, str(ul.id) ))).lstrip('-'),
                                                'richtungscode' : self.direction_mapper[ul.richtungskuerzel],
                                                'fzprofilname' : ul.unterliniennummer,
                                                'vonfzpelemindex' : f.start_pos,
                                                'nachfzpelemindex' : f.end_pos
                                          })

        fahrplanfahrtabschnitte = [{'nr': 1,
                                    'fplfahrtnr': x['nr'],
                                    'vonfzpelemindex' : x['vonfzpelemindex'],
                                    'nachfzpelemindex' : x['nachfzpelemindex']} \
                                    for x in fahrten]

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "FAHRPLANFAHRT" VALUES
                            (%(nr)s, %(name)s, %(abfahrt)s, %(linname)s, %(linroutename)s,
                             %(richtungscode)s, %(fzprofilname)s, %(vonfzpelemindex)s,
                             %(nachfzpelemindex)s)''',
                        fahrten)

        c.executemany('''INSERT INTO "FAHRPLANFAHRTABSCHNITT" VALUES
                            (%(nr)s, %(fplfahrtnr)s, %(vonfzpelemindex)s,
                             %(nachfzpelemindex)s)''',
                        fahrplanfahrtabschnitte)

        c.close()
        conn.commit()

        print '\tfinished converting Fahrplanfahrten'


    def _processVsysset(self):
        session = self._getNewSession()
        vsyssets_list = []
        self.default_vsysset = 'B'

##        for v in session.query(Vehicle_type).all():
##
##            vsyssets_list.append({  'code':v.str_vehicle_type,
##                                    'name':removeSpecialCharacter(v.veh_type_text),
##                                    'type':'OV',
##                                    'pkwe':1
##                                })

        vsyssets_list.append({  'code':'F',
                                'name':'Fuss',
                                'type':'OVFuss',
                                'pkwe':1
                            })
        vsyssets_list.append({  'code':'B',
                                'name':'Bus',
                                'type':'OV',
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

            if f.von_haltestelle.referenzhaltestelle != f.nach_haltestelle.referenzhaltestelle:
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
                                    'vsyscode' : 'Fuss',
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
    usage = """usage: python dinoToVisum.py [options] config_file dino_folder"""
    parser = OptionParser(usage=usage)

    parser.add_option("-e", "--export-only", action="store_true", help="converts the data in the database into the visum format (NO READING OF dino DATA)", dest="export_only", default=False)
    parser.add_option("-i", "--import-only", action="store_true", help="imports the dino data into the database (NO CONVERSION INTO VISUM DATA)", dest="import_only", default=False)

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

    dino_folder = args[1]
    dino_folder = r'D:\GIT\gs\graphserver_tools\graphserver_tools\msp\Eingangsdaten\01 Dino\Fahrplan DVG'
    read_dino_data = not options.export_only

    transformer = DinoToVisum(psql_connect_string, recreate_tables=False, read_dino_data=read_dino_data)
    transformer.dino_data = dino_folder
    transformer.date = datetime.date(int(config['date'][:4]), int(config['date'][5:7]), int(config['date'][8:]))

    if not options.import_only:
        print 'converting data'
        transformer.transform()

    print 'done'


if __name__ == '__main__': main()



