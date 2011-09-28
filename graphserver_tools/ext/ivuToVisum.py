import datetime
import os
import psycopg2
import sys

from sqlalchemy.orm import sessionmaker

from graphserver_tools.ext.ivu.models import *
from graphserver_tools.ext.ivu.read_ivu import read as readIvuToDb
from graphserver_tools.utils import utils



class ivuToVisum(object):

    def __init__(self, db_connect_string, date=datetime.datetime(2011,1,1), create_tables=False):
        self.db_connect_string = db_connect_string
        self.date = date

        if create_tables:
            self._createDbTables()


    #
    # setter & getter
    #
    def setIvuData(self, ivu_data):
        self._ivu_data = ivu_data

        #engine = create_engine('sqlite:///:memory:', echo=False)
        engine = create_engine('sqlite:///ivu.db', echo=False)

        Base.metadata.create_all(engine) # Base has been imported from ivu.models

        Session = sessionmaker(bind=engine)
        self._session = Session()

        readIvuToDb(ivu_data, self._session)


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
        self._createHaltestellenVsyssetMapper()
        self._getValidUnterlinien()


        print 'converting vertices'
        self._processKnoten()

        print 'converting stops'
        self._processHaltestelle()
        self._processHaltestellenbereich()
        self._processHaltepunkt()

        print 'converting routes'
        self._processLinie()
        self._processLinienroute()
        self._processLinienroutenelement()

        print 'converting trips'
        self._processFahrzeitprofil()
        self._processFahrzeitprofilelement()
        self._processFahrzeugfahrt()


    #
    # private methods
    #
    def _createDbTables(self):

        connection = psycopg2.connect(self.db_connect_string)
        cursor = connection.cursor()

        cursor.execute('''CREATE TABLE "FAHRZEITPROFIL"
                                (   "LINNAME" varchar(255),
                                    "LINROUTENAME" varchar(255),
                                    "RICHTUNGCODE" varchar(255),
                                    "NAME" varchar(255)
                                )''')

        cursor.execute('''CREATE TABLE "FAHRZEITPROFILELEMENT"
                                (   "LINNAME" varchar(255),
                                    "LINROUTENAME" varchar(255),
                                    "RICHTUNGCODE" varchar(255),
                                    "FZPROFILNAME" varchar(255),
                                    "INDEX" integer,
                                    "LRELEMINDEX" integer,
                                    "AUS" integer,
                                    "EIN" integer,
                                    "ANKUNFT" timestamp,
                                    "ABFAHRT" timestamp
                                )''')

        cursor.execute('''CREATE TABLE "FZGFAHRT"
                            (   "NR" integer,
                                "NAME" varchar(255),
                                "ABFAHRT" timestamp,
                                "LINNAME" varchar(255),
                                "LINROUTENAME" varchar(255),
                                "RICHTUNGCODE" varchar(255),
                                "FZPROFILNAME" varchar(255),
                                "VONFZPELEMINDEX" integer,
                                "NACHFZPELEMINDEX" integer
                            )''')

        cursor.execute('''CREATE TABLE "HALTEPUNKT"
                            (   "NR" integer,
                                "HSTBERNR" integer,
                                "CODE" varchar(255),
                                "NAME" varchar(255),
                                "TYPNR" integer,
                                "VSYSSET" varchar(255),
                                "DEPOTFZGKOMBMENGE" varchar(255),
                                "GERICHTET" integer,
                                "KNOTNR" integer,
                                "VONKNOTNR" integer,
                                "STRNR" integer,
                                "RELPOS" float
                            )''')

        cursor.execute('''CREATE TABLE "HALTESTELLE"
                            (   "NR" integer,
                                "CODE" varchar(255),
                                "NAME" varchar(255),
                                "TYPNR" integer,
                                "XKOORD" float,
                                "YKOORD" float
                            )''')

        cursor.execute('''CREATE TABLE "HALTESTELLENBEREICH"
                            (   "NR" integer,
                                "HSTNR" integer,
                                "CODE" varchar(255),
                                "NAME" varchar(255),
                                "KNOTNR" integer,
                                "TYPNR" integer,
                                "XKOORD" float,
                                "YKOORD" float
                            )''')

        cursor.execute('''CREATE TABLE "KNOTEN"
                            (   "NR" integer,
                                "XKOORD" float,
                                "YKOORD" float
                            )''')

        cursor.execute('''CREATE TABLE "LINIE"
                            (   "NAME" varchar(255),
                                "VSYSCODE" varchar(255),
                                "TARIFSYSTEMMENGE" varchar(255),
                                "BETREIBERNR" integer
                            )''')

        cursor.execute('''CREATE TABLE "LINIENROUTE"
                            (   "LINNAME" varchar(255),
                                "NAME" varchar(255),
                                "RICHTUNGCODE" varchar(255),
                                "ISTRINGLINIE" integer
                            )''')

        cursor.execute('''CREATE TABLE "LINIENROUTENELEMENT"
                            (   "LINNAME" varchar(255),
                                "LINROUTENAME" varchar(255),
                                "RICHTUNGCODE" varchar(255),
                                "INDEX" integer,
                                "ISTROUTENPUNKT" integer,
                                "KNOTNR" integer,
                                "HPUNKTNR" integer
                            )''')

        cursor.execute('''CREATE TABLE "STRECKE"
                            (   "NR" integer,
                                "VONKNOTNR" integer,
                                "NACHKNOTNR" integer,
                                "NAME" varchar(255),
                                "TYPNR" integer,
                                "VSYSSET" varchar(255)
                            )''')

        cursor.execute('''CREATE TABLE "STRECKENPOLY"
                            (   "VONKNOTNR" integer,
                                "NACHKNOTNR" integer,
                                "INDEX" integer,
                                "XKOORD" float,
                                "YKOORD" float
                            )''')

        cursor.execute('''CREATE TABLE "VERSION"
                            (   "VERSNR" float,
                                "FILETYPE" varchar(255),
                                "LANGUAGE" varchar(255)
                            )''')

        cursor.execute('INSERT INTO "VERSION" VALUES (%s, %s, %s)', ( 8.1, 'Net', 'DEU' ))

        cursor.close()
        connection.commit()


    def _getValidUnterlinien(self):
        # get all unterlinien
        unterlinien = self._session.query(Linie).all()

        # remove all unterlinen not valid on entered date
        unterlinien = [ ul for ul in unterlinien if ul.isValidOnDate(self.date) ]


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

        self.unterlinien =  [ ul for ul in unterlinien if ul.version == linien_version_mapper[( ul.liniennummer, ul.betrieb )] ]


    def _createHaltestellenVsyssetMapper(self):

        self.haltestellen_vsysset_mapper = { }
        ivu_haltestellen = self._session.query(Haltestelle).all()

        num_haltestellen = len(ivu_haltestellen)

        for i, h in enumerate(ivu_haltestellen):
            sys.stdout.write("\r\t%i of %i stops mapped" % ( i, num_haltestellen ))
            sys.stdout.flush()

            self.haltestellen_vsysset_mapper[h] = set([ l.linie.verkehrsmittel for l in h.linienprofile ])

        print ''


    def _processKnoten(self):
        ''' Method will write a vertex (Knoten) for every Haltestelle and Zwischenpunkt
            in the feed into the visum database.
        '''
        vertices = []

        haltestellen = self._session.query(Haltestelle).all()

        for h in haltestellen:
            if h.x_koordinate and h.y_koordinate:
                vertices.append({   'id':h.id,
                                    'xkoord':h.x_koordinate,
                                    'ykoord':h.y_koordinate
                                })


        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "KNOTEN" VALUES (%(id)s, %(xkoord)s, %(ykoord)s)''', vertices)

        vertices = []

        zwischenpunkte = self._session.query(Zwischenpunkt).all()

        self.zwischenpunkt_id_mapper = { } # maps between a zwischenpunkt and its id/NR in the visum database

        c.execute('''SELECT MAX("NR") FROM "KNOTEN"''')
        visum_id = c.fetchone()[0]

        for z in zwischenpunkte:
            visum_id += 1
            vertices.append({   'id':visum_id,
                                'xkoord':z.x_koordinate,
                                'ykoord':z.y_koordinate
                            })

            self.zwischenpunkt_id_mapper[z] = visum_id


        c.executemany('''INSERT INTO "KNOTEN" VALUES (%(id)s, %(xkoord)s, %(ykoord)s)''', vertices)

        c.close()
        conn.commit()


    def _processHaltestelle(self):
        ''' Method will write a Haltestelle for each IVU-Haltestelle with no
            referenzhaltestelle into the visum database.
        '''

        haltestellen_ivu = self._session.query(Haltestelle).filter(Haltestelle.referenzhaltestelle == None).all()
        haltestellen = []

        for h in haltestellen_ivu:
            haltestellen.append({   'nr': h.id,
                                    'code': h.haltestellenkuerzel,
                                    'name': h.haltestellennummer,
                                    'typnr': 1,
                                    'xkoord': h.x_koordinate,
                                    'ykoord': h.y_koordinate
                                })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "HALTESTELLE" VALUES
                            (%(nr)s, %(code)s, %(name)s, %(typnr)s, %(xkoord)s, %(ykoord)s)''',
                            haltestellen)

        c.close()
        conn.commit()


    def _processHaltestellenbereich(self):
        ''' Method will write a Haltestellenbereich for each IVU-Haltestelle with
            unterhaltestellen or no referenzhaltestelle.
        '''

        haltestellen_ivu = self._session.query(Haltestelle).filter(or_(Haltestelle.referenzhaltestelle == None, Haltestelle.unterhaltestellen != None)).all()
        hatestellenbereiche = []

        for h in haltestellen_ivu:

            hstnr = h.referenzhaltestelle.id if h.referenzhaltestelle else h.id

            hatestellenbereiche.append({    'nr': h.id,
                                            'hstnr': hstnr,
                                            'code': h.haltestellenkuerzel,
                                            'name': h.haltestellenlangname,
                                            'knotnr': h.id,
                                            'typnr': 1,
                                            'xkoord': h.x_koordinate,
                                            'ykoord': h.y_koordinate
                                        })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "HALTESTELLENBEREICH" VALUES
                            (%(nr)s, %(hstnr)s, %(code)s, %(name)s, %(knotnr)s, %(typnr)s, %(xkoord)s, %(ykoord)s)''',
                        hatestellenbereiche)

        c.close()
        conn.commit()


    def _processHaltepunkt(self):
        ''' Method will add a Haltepunkt to the visum database for each IVU-Haltestelle.
        '''

        haltestellen_ivu = self._session.query(Haltestelle).all()
        haltepunkte = []

        for h in haltestellen_ivu:

            hstbernr = h.referenzhaltestelle.id if h.referenzhaltestelle else h.id

            vsysset = ','.join([ v.verkehrsmittelname for v in self.haltestellen_vsysset_mapper[h] ])

            haltepunkte.append({    'nr' : h.id,
                                    'hstbernr' : hstbernr,
                                    'code' : h.haltestellenkuerzel,
                                    'name' : h.haltestellenlangname,
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


    def _processLinie(self):
        ''' Writes a Linie for each IVU-Linie (not Unterlinie) in the feed into the
            visum database.
        '''

        linien = []

        for linie, unterlinien in self.linien_unterlinien_mapper.items():

            linien.append({ 'name' : '-'.join(( linie[1].betriebsteilschluessel, linie[0] )),
                            'vsyscode' : unterlinien[0].verkehrsmittel.verkehrsmittelname,
                            'tarifsystemmenge' : 1,
                            'betreibernr' : linie[1].id
                          })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "LINIE" VALUES
                            (%(name)s, %(vsyscode)s, %(tarifsystemmenge)s, %(betreibernr)s)''',
                        linien)

        c.close()
        conn.commit()


    def _processLinienroute(self):
        ''' Writes a Linienroute for each different set of stops (trip) into the visum database.
        '''

        linienrouten = []

        for ul in self.unterlinien:

            linienrouten.append({   'linname' : '-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) )),
                                    'name' : '-'.join(( str(ul.unterliniennummer), ul.oeffentlicher_linienname )),
                                    'richtungscode' : ul.richtungskuerzel,
                                    'istringlinie' : 0
                                })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "LINIENROUTE" VALUES
                            (%(linname)s, %(name)s, %(richtungscode)s, %(istringlinie)s)''',
                        linienrouten)

        c.close()
        conn.commit()


    def _processLinienroutenelement(self):
        ''' Writes all Linienprofile of all valid Unterlinien to the visum database
        '''

        linienroutenelemente = []

        for ul in self.unterlinien:

            for lp in self._session.query(Linienprofil).filter(Linienprofil.linie == ul).all():

                linienroutenelemente.append({   'linname' : '-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) )),
                                                'linroutename' : '-'.join(( str(ul.unterliniennummer), ul.oeffentlicher_linienname )),
                                                'richtungscode' : ul.richtungskuerzel,
                                                'index' : lp.laufende_nummer,
                                                'istroutenpunkt' : 1,
                                                'knotnr' : lp.haltestelle.id,
                                                'hpunktnr' : lp.haltestelle.id
                                            })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "LINIENROUTENELEMENT" VALUES
                            (%(linname)s, %(linroutename)s, %(richtungscode)s, %(index)s,
                             %(istroutenpunkt)s, %(knotnr)s, %(hpunktnr)s)''',
                        linienroutenelemente)

        c.close()
        conn.commit()


    def _processFahrzeitprofil(self):
        ''' Writes a Fahrzeitprofil for each Unterlinie into the visum database.
        '''

        fahrzeitprofile = []

        for ul in self.unterlinien:

            fahrzeitprofile.append({    'linname' : '-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) )),
                                        'linroutename' : '-'.join(( str(ul.unterliniennummer), ul.oeffentlicher_linienname )),
                                        'richtungscode' : ul.richtungskuerzel,
                                        'name' : ul.unterliniennummer
                                   })


        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "FAHRZEITPROFIL" VALUES
                            (%(linname)s, %(linroutename)s, %(richtungscode)s, %(name)s)''',
                        fahrzeitprofile)

        c.close()
        conn.commit()


    def _processFahrzeitprofilelement(self):
        ''' Writes Fahrzeitprofilemente for Linienprofile of all valid Unterlinien into
            the visum database.
        '''
        elements = []

        for ul in self.unterlinien:

            travel_time_min = 0
            travel_time_hours = 0

            for lp in self._session.query(Linienprofil).filter(Linienprofil.linie == ul).all():

                travel_time_hours += lp.fahrzeit.hour
                travel_time_min += lp.fahrzeit.minute
                day = 30

                if travel_time_min > 59:
                    travel_time_min -= 60
                    travel_time_hours += 1

                if travel_time_hours > 23:
                    day += 1
                    travel_time_hours -= 24


                departure_time_min = travel_time_min + lp.wartezeit.minute
                departure_time_hour = travel_time_hours + lp.wartezeit.hour
                departure_day = day

                if departure_time_min > 59:
                    departure_time_min -= 60
                    departure_time_hour += 1

                if departure_time_hour > 23:
                    departure_day += 1
                    departure_time_hour -= 24


                arrival = datetime.datetime(1899, 12, day, travel_time_hours, travel_time_min)
                departure = datetime.datetime(1899, 12, departure_day, departure_time_hour, departure_time_min )

                elements.append({   'linname' : '-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) )),
                                    'linroutename' : '-'.join(( str(ul.unterliniennummer), ul.oeffentlicher_linienname )),
                                    'richtungscode' : ul.richtungskuerzel,
                                    'fzprofilname' : ul.unterliniennummer,
                                    'index' : lp.laufende_nummer,
                                    'lrelemindex' : lp.laufende_nummer,
                                    'aus' : int(not lp.aussteigeverbot),
                                    'ein' : int(not lp.einsteigeverbot),
                                    'ankunft' : arrival,
                                    'abfahrt' : departure,
                                })



        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "FAHRZEITPROFILELEMENT" VALUES
                            (%(linname)s, %(linroutename)s, %(richtungscode)s, %(fzprofilname)s,
                             %(index)s, %(lrelemindex)s, %(aus)s, %(ein)s, %(ankunft)s,
                             %(abfahrt)s)''',
                        elements)

        c.close()
        conn.commit()

    def _processFahrzeugfahrt(self):
        ''' Writes a Fahrzeugfahrt for each trip inside the feed into the visum database.
            Uses the pre-defined Fahrzeitprofile (fahrzeitprofil_mapper).
        '''
        fahrten = []

        for ul in self.unterlinien:

            for f in self._session.query(Fahrt).filter(Fahrt.linie == ul).all():

                if f.isValidOnDate(self.date):

                    departure_hour = int(f.abfahrt[:2])
                    departure_min = int(f.abfahrt[3:5])
                    departure_sec = int(f.abfahrt[6:]) if len(f.abfahrt) == 8 else 0


                    departure = datetime.datetime(1900, 1, 1, 0, 0, 0)

                    departure += datetime.timedelta(hours=departure_hour, minutes=departure_min, seconds=departure_sec)

                    fahrten.append({    'nr' : f.id,
                                        'name' : ul.oeffentlicher_linienname,
                                        'abfahrt' : departure,
                                        'linname' : '-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) )),
                                        'linroutename' : '-'.join(( str(ul.unterliniennummer), ul.oeffentlicher_linienname )),
                                        'richtungscode' : ul.richtungskuerzel,
                                        'fzprofilname' : ul.unterliniennummer,
                                        'vonfzpelemindex' : f.start_pos,
                                        'nachfzpelemindex' : f.end_pos
                                  })

                    # add folgefahrten
                    if f.zeitspanne:

                        for i in range(f.anzahl_folgefahrten):

                            departure += datetime.timedelta(hours=f.zeitspanne.hours, minutes=f.zeitspanne.minutes)


                            fahrten.append({    'nr' : f.id,
                                                'name' : ul.oeffentlicher_linienname,
                                                'abfahrt' : departure,
                                                'linname' : '-'.join(( ul.betrieb.betriebsteilschluessel, str(ul.liniennummer) )),
                                                'linroutename' : '-'.join(( str(ul.unterliniennummer), ul.oeffentlicher_linienname )),
                                                'richtungscode' : ul.richtungskuerzel,
                                                'fzprofilname' : ul.unterliniennummer,
                                                'vonfzpelemindex' : f.start_pos,
                                                'nachfzpelemindex' : f.end_pos
                                          })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "FZGFAHRT" VALUES
                            (%(nr)s, %(name)s, %(abfahrt)s, %(linname)s, %(linroutename)s,
                             %(richtungscode)s, %(fzprofilname)s, %(vonfzpelemindex)s,
                             %(nachfzpelemindex)s)''',
                        fahrten)

        c.close()
        conn.commit()



def main():
    from optparse import OptionParser
    usage = """usage: python ivuToVisum.py config_file ivu_folder"""
    parser = OptionParser(usage=usage)
    (options, args) = parser.parse_args()


    defaults = { 'psql-host':'localhost',
                 'psql-port':'5432',
                 'psql-user':'postgres',
                 'psql-password':'',
                 'psql-database':'graphserver',
                 'date':'2011.01.01' }

    if not os.path.exists(args[0]): raise Exception()

    config = utils.read_config(args[0], defaults, True)

    psql_connect_string = 'dbname=%s user=%s password=%s host=%s port=%s' % ( config['psql-database'],
                                                                              config['psql-user'],
                                                                              config['psql-password'],
                                                                              config['psql-host'],
                                                                              config['psql-port']       )

    ivu_folder = args[1]

    transformer = ivuToVisum(psql_connect_string, create_tables=True)
    transformer.ivu_data = ivu_folder
    transformer.date = datetime.date(int(config['date'][:4]), int(config['date'][5:7]), int(config['date'][8:]))

    print 'converting data'
    transformer.transform()

    print 'done'


if __name__ == '__main__': main()


