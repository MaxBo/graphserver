#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 26.07.2011
# Gertz Gutsche Rümenapp Gbr


import datetime
import os
import psycopg2

import transitfeed

from graphserver_tools.ext.visumPuTTables import VisumPuTTables
from graphserver_tools.utils import utils



class GtfsToVisum(VisumPuTTables):

    route_type_mapper = {   0 : 'Tram/Light rail',
                            1 : 'Subway',
                            2 : 'Railway',
                            3 : 'Bus',
                            4 : 'Ferry',
                            5 : 'Cable Car',
                            6 : 'Gondola',
                            7 : 'Funicular'
                        }

    direction_mapper = {    '1' : '>',
                            '0' : '<'
                       }


    def __init__(self, db_connect_string, date='20110101', recreate_tables=False):
        self.db_connect_string = db_connect_string
        self.date = date

        self._createDbTables(recreate_tables)
        self._truncateDbTables()


    #
    # setter & getter
    #
    def setFeed(self, feed):
        self._feed = feed

        factory = transitfeed.GetGtfsFactory()
        loader = factory.Loader(self._feed)

        self._schedule = loader.Load()

        self._createStopIdMapper()
        self._createLinrouteMapper()
        self._createFahrzeitprofilMapper()

    def getFeed(self):
        return self._feed

    feed = property(getFeed, setFeed)


    #
    # other public methods
    #
    def transform(self):
        ''' Converts the feed associated with this object into a data for a visum database.
        '''

        self._createStopIdMapper()
        self._createLinrouteMapper()
        self._createFahrzeitprofilMapper()
        self._processVerkehrssysteme()
        self._processBetreiber()
        self._processKnoten()
        self._processHaltestelle()
        self._processHaltestellenbereich()
        self._processHaltepunkt()
        self._processLinie()
        self._processLinienroute()
        self._processLinienroutenelement()
        self._processFahrzeitprofil()
        self._processFahrzeitprofilelement()
        self._processFahrplanfahrt()


    #
    # private methods
    #

    def _createStopIdMapper(self):
        ''' The visum id (NR) is integer while gtfs id (stop_id) is string.
            This method creates a dictionary to map between those to ids.
        '''

        self.stop_id_mapper = {}
        id_counter = 0

        for s in self._schedule.GetStopList():
            self.stop_id_mapper[s.stop_id] = id_counter
            id_counter += 1


    def _stripStopTimesTuples(self, tuples):
        ''' Removes all trip specific elements from a StopTimesTuples list and replaces time string
            so that the departure time at the first stop will be 00:00:00.
            Result: [ ( arrival, departure, stop_id, stop_sequence), … ) ]
        '''

        def timeDownStripper(time_string):
            hour = int(start_time[0:2]) - int(time_string[0:2])
            minute = int(start_time[3:5]) - int(time_string[3:5])
            second = int(start_time[6:8]) - int(time_string[6:8])

            return '%02d:%02d:%02d' % (hour, minute, second)


        ret_tuples = []
        start_time = tuples[0][1]

        for t in tuples:
            ret_tuples.append(( timeDownStripper(t[1]),
                                timeDownStripper(t[2]),
                                t[3], t[4]
                             ))

        return tuple(ret_tuples)


    def _createFahrzeitprofilMapper(self):
        ''' Creates a dictionary mapping between a (linname, linroutename, fzprofilname) and
            trip_ids.
            Groupes all trips to one fzprofil which share the same stops and stop_times.
            Needs linroute_mapper the produce proper results.
        '''

        self.fahrzeitprofil_mapper = {}


        for (linname, linroutename), trip_ids in self.linroute_mapper.items():
            stops_fahrzeitprofil_mapper = {} # maps between list of stops/_times and linroutenames
            fahrzeit_counter = 1

            for id in trip_ids:
                trip = self._schedule.GetTrip(id)

                trip_stop_times = self._stripStopTimesTuples(trip.GetStopTimesTuples())

                if not trip_stop_times in stops_fahrzeitprofil_mapper:

                    fahrzeitprofil_identifier = (linname, linroutename, fahrzeit_counter)
                    fahrzeit_counter += 1

                    stops_fahrzeitprofil_mapper[trip_stop_times] = fahrzeitprofil_identifier
                    self.fahrzeitprofil_mapper[fahrzeitprofil_identifier] = [ trip.trip_id, ]

                else:
                    key = stops_fahrzeitprofil_mapper[trip_stop_times]
                    self.fahrzeitprofil_mapper[key].append(trip.trip_id)


    def _createLinrouteMapper(self):
        ''' Creates a dictionary mapping between a (linname, linroutename) and trip_ids.
            Groupes all trips to one linroute which share the same stops (not stop_times).
        '''

        self.linroute_mapper = {}

        for route in self._schedule.GetRouteList():
            stops_linroute_mapper = {} # maps between list of stops and linroutenames
            linroute_counter = 1

            for trip in route._trips:
                trip_stops = tuple([ st.stop for st in trip.GetStopTimes() ])

                if not trip_stops in stops_linroute_mapper:
                    direction = self.direction_mapper.get(trip.direction_id, '>')

                    linroutename = str(linroute_counter) + '_' + direction
                    stops_linroute_mapper[trip_stops] = linroutename

                    linroute_counter += 1

                    self.linroute_mapper[( route.route_id, linroutename)] = [ trip.trip_id, ]

                else:
                    key = ( route.route_id, stops_linroute_mapper[trip_stops] )
                    self.linroute_mapper[key].append(trip.trip_id)


    def _processVerkehrssysteme(self):

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.execute('INSERT INTO "VSYS" VALUES (%s, %s, %s, %s)', ('Tram/Light rail', 'Tram/Light rail', 'OV', 1))
        c.execute('INSERT INTO "VSYS" VALUES (%s, %s, %s, %s)', ('Subway', 'Subway', 'OV', 1))
        c.execute('INSERT INTO "VSYS" VALUES (%s, %s, %s, %s)', ('Railway', 'Railway', 'OV', 1))
        c.execute('INSERT INTO "VSYS" VALUES (%s, %s, %s, %s)', ('Bus', 'Bus', 'OV', 1))
        c.execute('INSERT INTO "VSYS" VALUES (%s, %s, %s, %s)', ('Ferry', 'Ferry', 'OV', 1))
        c.execute('INSERT INTO "VSYS" VALUES (%s, %s, %s, %s)', ('Cable Car', 'Cable Car', 'OV', 1))
        c.execute('INSERT INTO "VSYS" VALUES (%s, %s, %s, %s)', ('Gondola', 'Gondola', 'OV', 1))
        c.execute('INSERT INTO "VSYS" VALUES (%s, %s, %s, %s)', ('Funicular', 'Funicular', 'OV', 1))

        c.close()
        conn.commit()


    def _processKnoten(self):
        ''' Method will write a vertex (Knoten) for every stop (not station) in the feed
            into the visum database. It will need the stop_id_mapper
        '''

        stops = [s for s in self._schedule.GetStopList() if not s.location_type]
        vertices = []

        for s in stops:
            if not s.location_type: # stations don't need a vertex
                vertices.append({   'id':self.stop_id_mapper[s.stop_id],
                                    'xkoord':s.stop_lat,
                                    'ykoord':s.stop_lon
                                })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "KNOTEN" VALUES (%(id)s, %(xkoord)s, %(ykoord)s)''', vertices)

        c.close()
        conn.commit()


    def _processBetreiber(self):
        ''' Method will write a Betreiber into the visum database for each agency found
            in the gtfs feed.
        '''

        agencies = self._schedule.GetAgencyList()

        betreiber = []

        for a in agencies:
            betreiber.append({  'nr' : a.agency_id,
                                'name' : a.agency_name,
                                'kosten1' : 0,
                                'kosten2' : 0,
                                'kosten3' : 0
                            })


        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "BETREIBER" VALUES (%(nr)s, %(name)s, %(kosten1)s, %(kosten2)s, %(kosten3)s)''', betreiber)

        c.close()
        conn.commit()


    def _processHaltestelle(self):
        ''' Method will write a Haltestelle into the visum database for each station in the feed.
            It will need the stop_id_mapper
        '''

        stations = [s for s in self._schedule.GetStopList() if s.location_type]
        haltestellen = []

        for s in stations:
            haltestellen.append({   'nr': self.stop_id_mapper[s.stop_id],
                                    'code': s.stop_name[:10],
                                    'name': s.stop_name,
                                    'typnr': 1,
                                    'xkoord': s.stop_lat,
                                    'ykoord': s.stop_lon
                                })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "HALTESTELLE" VALUES
                            (%(nr)s, %(code)s, %(name)s, %(typnr)s, %(xkoord)s, %(ykoord)s)''',
                            haltestellen)

        c.close()
        conn.commit()


    def _processHaltestellenbereich(self):
        ''' Method will write a Haltestellenbereich and a Haltestelle (if necessary) for each
            stop inside the feed. It will need the stop_id_mapper.
        '''
        stops = [s for s in self._schedule.GetStopList() if not s.location_type]

        haltestellen = []
        hatestellenbereiche = []

        for s in stops:
            if not s.parent_station:
                # create a new haltestelle

                haltestellen.append({   'nr': self.stop_id_mapper[s.stop_id],
                                        'code': s.stop_name[:10],
                                        'name': s.stop_name,
                                        'typnr': 1,
                                        'xkoord': s.stop_lat,
                                        'ykoord': s.stop_lon
                                    })
                hstnr = self.stop_id_mapper[s.stop_id]
            else:
                hstnr = self.stop_id_mapper[s.parent_station]

            hatestellenbereiche.append({   'nr': self.stop_id_mapper[s.stop_id],
                                            'hstnr': hstnr,
                                            'code': s.stop_name[:10],
                                            'name': s.stop_name,
                                            'knotnr': self.stop_id_mapper[s.stop_id],
                                            'typnr': 1,
                                            'xkoord': s.stop_lat,
                                            'ykoord': s.stop_lon
                                        })

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "HALTESTELLE" VALUES
                            (%(nr)s, %(code)s, %(name)s, %(typnr)s, %(xkoord)s, %(ykoord)s)''',
                        haltestellen)

        c.executemany('''INSERT INTO "HALTESTELLENBEREICH" VALUES
                            (%(nr)s, %(hstnr)s, %(code)s, %(name)s, %(knotnr)s, %(typnr)s, %(xkoord)s, %(ykoord)s)''',
                        hatestellenbereiche)

        c.close()
        conn.commit()


    def _processHaltepunkt(self):
        ''' Method will add a Haltepunkt to the visum database for each stop (not station) in the feed.
            It will nee the stop_id_mapper. Checks which route_type is suitable for the corresponding
            stop/Haltepunkt.
        '''

        # create a dictionary containing stops and corresponding transit_types
        stop_transit_type_mapper = {}

        for t in self._schedule.GetTripList():
            route_type = self._schedule.GetRoute(t.route_id).route_type

            for st in t.GetStopTimes():
                s = st.stop

                if not s in stop_transit_type_mapper:
                    stop_transit_type_mapper[s] = set()

                stop_transit_type_mapper[s].add(route_type)


        # determine hatltepunkte
        stops = [s for s in self._schedule.GetStopList() if not s.location_type]
        haltepunkte = []

        for s in stops:

            if s in stop_transit_type_mapper:
                vsysset = ','.join([ self.route_type_mapper[rt] for rt in stop_transit_type_mapper[s] ])
            else:
                vsysset = None

            haltepunkte.append({    'nr' : self.stop_id_mapper[s.stop_id],
                                    'hstbernr' : self.stop_id_mapper[s.stop_id],
                                    'code' : s.stop_name[:10],
                                    'name' : s.stop_name,
                                    'typnr' : 1,
                                    'vsysset' : vsysset,
                                    'depotfzgkombm' : None,
                                    'gerichtet' : 1,
                                    'knotnr' : self.stop_id_mapper[s.stop_id],
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
        ''' Writes a Linie for each route in the feed into the visum database. Maps from route_type to
            human readable text.
        '''

        linien = []

        for r in self._schedule.GetRouteList():
            linien.append({ 'name' : r.route_id,
                            'vsyscode' : self.route_type_mapper[r.route_type],
                            'tarifsystemmenge' : 1,
                            'betreibernr' : r.agency_id
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

        for linname, name in self.linroute_mapper:

            direction_id = self._schedule.GetTrip(self.linroute_mapper[( linname, name )][0]).direction_id
            direction = self.direction_mapper[direction_id] if direction_id else '>'

            linienrouten.append({   'linname' : linname,
                                    'name' : name,
                                    'richtungscode' : direction,
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
        ''' Writes for each pre-processed Linroute (createLinrouteMapper) and it's stops
            (linroutelements) an entry into the visum database
        '''

        linienroutenelemente = []

        for (linname, linroutename), trip_ids in self.linroute_mapper.items():

            trip = self._schedule.GetTrip(trip_ids[0])

            direction_id = trip.direction_id
            direction = self.direction_mapper[direction_id] if direction_id else '>'

            for st in trip.GetStopTimes():
                linienroutenelemente.append({   'linname' : linname,
                                                'linroutename' : linroutename,
                                                'richtungscode' : direction,
                                                'index' : st.stop_sequence,
                                                'istroutenpunkt' : 1,
                                                'knotnr' : self.stop_id_mapper[st.stop.stop_id],
                                                'hpunktnr' : self.stop_id_mapper[st.stop.stop_id]
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
        ''' Writes a Fahrzeitprofil for each different set of stops/stop_time combination
            into the visum database.
        '''

        fahrzeitprofile = []

        for (linname, linroutename, fzprofilname), trip_ids in self.fahrzeitprofil_mapper.items():
            trip = self._schedule.GetTrip(trip_ids[0])

            direction_id = trip.direction_id
            direction = self.direction_mapper[direction_id] if direction_id else '>'

            fahrzeitprofile.append({    'linname' : linname,
                                        'linroutename' : linroutename,
                                        'richtungscode' : direction,
                                        'name' : fzprofilname
                                   })


        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "FAHRZEITPROFIL" VALUES
                            (%(linname)s, %(linroutename)s, %(richtungscode)s, %(name)s)''',
                        fahrzeitprofile)

        c.close()
        conn.commit()


    def _processFahrzeitprofilelement(self):
        ''' Writes Fahrzeitprofilemente for each stop of each pre-defined Fahrzeitprofil
            (fahrzeitprofil_mapper) into the visum database.
        '''
        elements = []

        for (linname, linroutename, fzprofilname), trip_ids in self.fahrzeitprofil_mapper.items():

            direction_id = self._schedule.GetTrip(trip_ids[0]).direction_id
            direction = self.direction_mapper[direction_id] if direction_id else '>'

            trip = self._schedule.GetTrip(trip_ids[0])

            start_time = trip.GetStartTime() + 2209165200 # make the result on 1899-12-30

            for st in trip.GetStopTimes():

                pickup_type = st.pickup_type if st.pickup_type else 1
                drop_off_type = st.drop_off_type if st.drop_off_type else 1

                arrival = datetime.datetime.fromtimestamp(st.arrival_secs - start_time)
                departure = datetime.datetime.fromtimestamp(st.departure_secs - start_time)

                elements.append({   'linname' : linname,
                                    'linroutename' : linroutename,
                                    'richtungscode' : direction,
                                    'fzprofilname' : fzprofilname,
                                    'index' : st.stop_sequence,
                                    'lrelemindex' : st.stop_sequence,
                                    'aus' : pickup_type,
                                    'ein' : drop_off_type,
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

    def _processFahrplanfahrt(self):
        ''' Writes a Fahrzeugfahrt for each trip inside the feed into the visum database.
            Uses the pre-defined Fahrzeitprofile (fahrzeitprofil_mapper).
        '''
        fahrten = []

        nr = 1

        for (linname, linroutename, fzprofilname), trip_ids in self.fahrzeitprofil_mapper.items():

            direction_id = self._schedule.GetTrip(trip_ids[0]).direction_id
            direction = self.direction_mapper[direction_id] if direction_id else '>'

            for id in trip_ids:
                trip = self._schedule.GetTrip(id)

                # only put trips into visum that which are valid on the selected date
                if self.date in trip.service_period.ActiveDates():

                    departure = datetime.datetime.fromtimestamp(trip.GetStartTime() - 2209165200)
                    name = trip.trip_headsign if trip.trip_headsign else None

                    fahrten.append({    'nr' : nr,
                                        'name' : name,
                                        'abfahrt' : departure,
                                        'linname' : linname,
                                        'linroutename' : linroutename,
                                        'richtungscode' : direction,
                                        'fzprofilname' : fzprofilname,
                                        'vonfzpelemindex' : 1,
                                        'nachfzpelemindex' : trip.GetCountStopTimes()
                                  })

                    nr += 1

                    # add frequency trips
                    for i, st in enumerate(trip.GetFrequencyStartTimes()):
                        st_departure = datetime.datetime.fromtimestamp(st - 2209165200)

                        if st_departure != departure:

                            fahrten.append({    'nr' : nr,
                                                'name' : name,
                                                'abfahrt' : st_departure,
                                                'linname' : linname,
                                                'linroutename' : linroutename,
                                                'richtungscode' : direction,
                                                'fzprofilname' : fzprofilname,
                                                'vonfzpelemindex' : 1,
                                                'nachfzpelemindex' : trip.GetCountStopTimes()
                                          })
                            nr += 1

        conn = psycopg2.connect(self.db_connect_string)
        c = conn.cursor()

        c.executemany('''INSERT INTO "FAHRPLANFAHRT" VALUES
                            (%(nr)s, %(name)s, %(abfahrt)s, %(linname)s, %(linroutename)s,
                             %(richtungscode)s, %(fzprofilname)s, %(vonfzpelemindex)s,
                             %(nachfzpelemindex)s)''',
                        fahrten)

        c.close()
        conn.commit()



def main():
    from optparse import OptionParser
    usage = """usage: python netToVisum.py config_file gtfs_feed"""
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

    feed = args[1]

    transformer = GtfsToVisum(psql_connect_string, recreate_tables=False)
    transformer.feed = feed
    transformer.date = config['date'].replace('.','')

    transformer.transform()

    print 'done'


if __name__ == '__main__': main()




