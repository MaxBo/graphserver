#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 26.07.2011
# Gertz Gutsche Rümenapp Gbr


import transitfeed
import psycopg2



class GtfsToNet(object):

    def __init__(self, feed, db_connect_string, create_tables=False):

        self.db_connect_string = db_connect_string

        if create_db_tables:
            self.create_db_tables()

        factory = transitfeed.GetGtfsFactory()
        loader = factory.Loader('transit_data.zip')
        self.schedule = loader.Load()

        route_type_mapper = {   0 : 'Tram/Light rail',
                                1 : 'Subway',
                                2 : 'Railway',
                                3 : 'Bus',
                                4 : 'Ferry',
                                5 : 'Cable Car',
                                6 : 'Gondola',
                                7 : 'Funicular'
                            }


    def create_db_tables(self):

        connection = psycopg2.connect(self.connect_string)
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

        cusor.execute('''CREATE TABLE "HALTEPUNKT"
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

        cursor.execute('INSERT INTO version VALUES (8.1, "Net", "DEU")')

        cursor.close()
        connection.commit()


    def createStopIdMapper(self):
        ''' The visum id (NR) is integer while gtfs id (stop_id) is string.
            This method creates a dictionary to map between those to ids.
        '''

        self.stop_id_mapper = {}
        id_counter = 0

        for s in self.schedule.GetStopList():
            self.stop_id_mapper[s.id] = id_counter
            id_counter += 1

    def processKnoten(self):
        ''' Method will write a vertex (Knoten) for every stop in the feed
            into the visum database. It will need the stop_id_mapper
        '''

        stops = [s for s in self.schedule.GetStopList() if not s.location_type]
        vertices = []

        for s in stops:
            vertices.append({   'id':self.stop_id_mapper[s.stop_id],
                                'xkoord':s.stop_lat,
                                'ykoord':s.stop_lon
                            })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO knoten VALUES (%(id)s, %(xkoord)s, %(ykoord)s)''', vertices)

        c.close()
        conn.commit()


    def processHaltestelle(self):
        ''' Method will write a Haltestelle into the visum database for each station in the feed.
            It will need the stop_id_mapper
        '''

        stations = [s for s in self.schedule.GetStopList() if s.location_type]
        haltestellen = []

        for s in stations:
            haltestellen.append({   'nr': self.stop_id_mapper[s.stop_id],
                                    'code': s.stop_name[:10],
                                    'name': s.stop_name,
                                    'typnr': 1
                                    'xkoord': s.stop_lat
                                    'ykoord': s.stop_lon
                                })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO haltestelle VALUES
                            (%(nr)s, %(code)s, %(name)s, %(typnr)s, %(xkoord)s, %(ykoord)s''',
                            haltestellen)

        c.close()
        conn.commit()


    def processHaltestellenbereich(self):
        ''' Method will write a Haltestellenbereich and a Haltestelle (if necessary) for each
            stop inside the feed. It will need the stop_id_mapper.
        '''
        stops = [s for s in self.schedule.GetStopList() if not s.location_type]

        haltestellen = []
        hatestellenbereiche = []

        for s in stops:
            if not s.parent_station:
                # create a new haltestelle

                haltestellen.append({   'nr': self.stop_id_mapper[s.stop_id],
                                        'code': s.stop_name[:10],
                                        'name': s.stop_name,
                                        'typnr': 1
                                        'xkoord': s.stop_lat
                                        'ykoord': s.stop_lon
                                    })
                hstnr = self.stop_id_mapper[s.id]
            else:
                hstnr = self.stop_id_mapper[s.parent_station]

            haltestellenbereich.append({    'nr': self.stop_id_mapper[s.stop_id],
                                            'hstnr': hstnr,
                                            'code': s.stop_name[:10],
                                            'name': s.stop_name,
                                            'knotnr': self.stop_id_mapper[s.stop_id],
                                            'typnr': 1,
                                            'xkoord': s.stop_lat
                                            'ykoord': s.stop_lon
                                        })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO haltestelle VALUES
                            (%(nr)s, %(code)s, %(name)s, %(typnr)s, %(xkoord)s, %(ykoord)s''',
                        haltestellen)

        c.executemany('''INSERT INTO haltestellenbereich VALUES
                            (%(nr)s, %(hstnr)s, %(code)s, %(name)s, %(knotnr)s, %(typnr)s, %(xkoord)s, %(ykoord)s''',
                        haltestellenbereich)

        c.close()
        conn.commit()


    def processHaltepunkt(self):
        ''' Method will add a Haltepunkt to the visum database for each stop (not station) in the feed.
            It will nee the stop_id_mapper. Checks which route_type is suitable for the corresponding
            stop/Haltepunkt.
        '''

        # create a dictionary containing stops and corresponding transit_types
        stop_transit_type_mapper = {}

        for t in self.schedule.GetTripList():
            route_type = self.schedule.GetRoute(t.route_id).transit_type

            for st in t.GetStopTimes():
                s = st.stop

                if not s in stop_id_mapper:
                    stop_transit_type_mapper[s] = set()

                stop_transit_type_mapper[s].add(route_type)


        # determine hatltepunkte
        stops = [s for s in self.schedule.GetStopList() if not s.location_type]
        haltepunkte = []

        for s in stops:
            haltepunkte.append({    'nr' : self.stop_id_mapper[s.stop_id],
                                    'hstbernr' : self.stop_id_mapper[s.stop_id],
                                    'code' : s.stop_name[:10],
                                    'name' : s.stop_name,
                                    'typnr' : 1,
                                    'vsysset' : ','.join([ self.route_type_mapper[rt] for rt in stop_transit_type_mapper[s] ]),
                                    'depotfzgkombm' : None,
                                    'gerichtet' : 1,
                                    'knotnr' : self.stop_id_mapper[s.stop_id],
                                    'vonknotnr' : None,
                                    'strnr' : None,
                                    'relpos' : 0,
                                })
        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO haltepunkt VALUES
                            (%(nr)s, %(hstbernr)s, %(code)s, %(name)s, %(typnr)s, %(vsysset)s,
                            %(depotfzgkombm)s, %(gerichtet)s, %(kontnr)s, %(vonknotnr)s, %(strnr)s, %(relpos)s)''',
                        haltepunkte)

        c.close()
        conn.commit()


    def processLinie(self):
        ''' Writes a Linie for each route in the feed into the visum database. Maps from route_type to
            human readable text.
        '''

        linien = []

        for r in schedule.GetRoutesList():
            linien.appedn({ 'name' : r.route_id
                            'vsyscode' : self.route_type_mapper[r.route_type],
                            'tarifsystemmenge' : 1,
                            'betreibernr' : r.agency_id
                          }

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO linie VALUES
                            (%(name)s, %(vsyscode)s, %(tarifsystemmenge)s, %(betreibernr)s)''',
                        linien)

        c.close()
        conn.commit()


    def processLinienroute(self):
        ''' Writes a Linienroute for each direction (if defined in corresponding trips) to the visum
            database
        '''

        # find all directions of all routes
        routes_directions = {}

        for t in self.GetTripList():
            if not t.route_id in routes_directions:
                routes_directions[t.route_id] = set()

            routes_directions.add(t.direction_id)


        # create all Linienrouten
        linienrouten = []
        direction_mapper = { '1':'<', '0':'>' }

        for r in self.schedule.GetRoutesList():
            if not r.route_id in routes_directions:

                linienrouten.append({   'linname' : r.route_id,
                                        'name' : str(r.route_id) + '_<',
                                        'richtungscode' : '<',
                                        'istringlinie' : 0
                                    })

            else:
                for direction in routes_directions[r.route_id]:

                    linienrouten.append({   'linname' : r.route_id,
                                            'name' : str(r.route_id) + '_' + direction_mapper[direction],
                                            'richtungscode' : direction,
                                            'istringlinie' : 0
                                        })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO linienroute VALUES
                            (%(linname)s, %(name)s, %(richtungscode)s, %(istringlinie)s)''',
                        linienrouten)

        c.close()
        conn.commit()




