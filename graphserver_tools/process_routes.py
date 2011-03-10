#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 24.10.2010
# Gertz Gutsche Rümenapp Gbr


import sqlite3
import datetime
import time
import sys
from graphserver.core import WalkOptions, State


class Proccessing():
    def get_gs_vertex(self, point_id):
        self.cursor.execute('SELECT vertex_label FROM corres_vertices WHERE point_id=?', ( point_id, ))
        return self.cursor.fetchone()[0]


    def get_point(self, vertex_label):
        self.cursor.execute('SELECT point_id FROM corres_vertices WHERE vertex_label=?', ( vertex_label, ))
        return [x[0] for x in self.cursor][0]


    def prepare_times(self, start_time, end_time):
        times = []

        start = time.mktime(start_time.timetuple())
        end = time.mktime(end_time.timetuple())

        t = start
        while t <= end:
            times.append(t)
            t += self.time_step

        return times


    def create_db_tables(self):


        self.cursor.execute('''CREATE TABLE IF NOT EXISTS trips ( id INTEGER PRIMARY KEY,
                                                                  route_id INTEGER,
                                                                  start_time TIMESTAMP,
                                                                  end_time TIMESTAMP,
                                                                  total_time INTEGER)''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS trip_details ( trip_id INTEGER,
                                                                         counter INTEGER,
                                                                         label TEXT,
                                                                         time TIMESTAMP,
                                                                         weight INTEGER,
                                                                         dist_walked REAL,
                                                                         num_transfers INTEGER,
                                                                         gtfs_trip_id TEXT)''')

        self.cursor.execute('CREATE INDEX IF NOT EXISTS IDX_time ON routes ( time )')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS IDX_origin ON routes ( origin )')

        self.conn.commit()


    def get_route_dict(self):
        try:

            self.cursor.execute('SELECT origin, destination, time FROM routes WHERE done=? LIMIT 1', ( 0, ))
            origin, destination, time = self.cursor.fetchone()

        except: # there are no routes to Compute
            return None

        start_time, end_time, is_arrival = self.cursor.execute('SELECT start, end, is_arrival_time FROM times WHERE id=?', ( time, )).fetchone()

        if is_arrival:
            return self.get_retro_dict(destination, time, start_time, end_time)

        else:
            return self.get_dict(origin, time, start_time, end_time)


    def get_retro_dict(self, destination, time, start_time, end_time):
        origins = list(self.cursor.execute('''SELECT id, origin FROM routes WHERE destination=? AND time=?''', ( destination, time )).fetchall())

        self.cursor.execute('UPDATE routes SET done=? WHERE destination=? AND time=?', ( 1, destination, time ))
        self.conn.commit()

        return { 'destination':self.get_gs_vertex(destination), 'times':self.prepare_times(start_time, end_time),
                 'arrival':True,
                 'origins':[ ( self.get_gs_vertex(orig[1]), orig[0] ) for orig in origins ] }


    def get_dict(self, origin, time, start_time, end_time):
        destinations = list(self.cursor.execute('''SELECT id, destination FROM routes WHERE origin=? AND time=?''', ( origin, time )).fetchall())

        self.cursor.execute('UPDATE routes SET done=? WHERE origin=? AND time=?', ( 1, origin, time ))
        self.conn.commit()

        return { 'origin':self.get_gs_vertex(origin), 'times':self.prepare_times(start_time, end_time),
                 'arrival':False,
                 'destinations':[ ( self.get_gs_vertex(dest[1]), dest[0] ) for dest in destinations ] }


    def process_trips(self, routes):
        for t in routes['times']:
            s = State(1, t)

            # build the shortest path tree at time 't'
            try:
                if len(routes['destinations']) > 1:
                    spt = self.graph.shortest_path_tree(routes['origin'], None, s, self.walk_ops)
                else:
                    spt = self.graph.shortest_path_tree(routes['origin'],routes['destinations'][0][0], s, self.walk_ops) # faster but only ONE destination
            except:
                pass

            # extract the actual routes and write them into the database
            for dest in routes['destinations']:
                try:
                    vertices, edges = spt.path(dest[0])

                    if not vertices: raise Exception()

                except:
                    self.write_error_trip(t, dest[1])
                else:
                    self.write_trip(vertices, dest[1])

            # cleanup
            try:
                spt.destroy()
            except:
                pass


    def process_retro_trips(self, routes):
        for t in routes['times']:
            s = State(1, t)

            # build the shortest path tree at time 't'
            try:
                if len(routes['origins']) > 1:
                    spt = self.graph.shortest_path_tree_retro(None, routes['destination'], s,self.walk_ops)
                else:
                    spt = self.graph.shortest_path_tree_retro(routes['origins'][0][0], routes['destination'], s, self.walk_ops) # faster but only ONE destination
            except:
                pass

            # extract the actual routes and write them into the database
            for orig in routes['origins']:
                try:
                    vertices, edges = spt.path_retro(orig[0])

                    if not vertices: raise Exception()

                except:
                    self.write_error_trip(t, orig[1])
                else:
                    self.write_retro_trip(vertices, orig[1])

            # cleanup
            try:
                spt.destroy()
            except:
                pass


    '''
        method for processing (calculating shortest paths) all routes stored inside the databases
        associated with this object.
        [only routes with the processed flag not set will be processed]
    '''
    def process(self):

        num_all_routes = len(self.cursor.execute('SELECT * FROM routes').fetchall())
        num_proccessed_routes = 0

        routes = self.get_route_dict()
        while ( routes ):
            sys.stdout.write('\r%s/%s shortest paths found' % ( num_proccessed_routes, num_all_routes ))
            sys.stdout.flush()


            if routes['arrival']:
                self.process_retro_trips(routes)
                num_proccessed_routes += len(routes['origins'])

            else:
                self.process_trips(routes)
                num_proccessed_routes += len(routes['destinations'])


            self.conn.commit()

            routes = self.get_route_dict()

        print('\r%s shortest paths found                     ' % num_all_routes )


    ''' in retro_paths the walking distance is counted in the wrong direction.
        this method corrects this.
    '''
    def write_retro_trip(self, vertices, route_id):
        total_dist_walked = vertices[0].state.dist_walked

        for v in vertices:
            v.state.dist_walked = total_dist_walked - v.state.dist_walked

        self.write_trip(vertices, route_id)


    def write_trip(self, vertices, route_id):
        start_time = datetime.datetime.fromtimestamp(vertices[0].state.time)
        end_time = datetime.datetime.fromtimestamp(vertices[-1].state.time)

        self.cursor.execute('INSERT INTO trips VALUES (?,?,?,?,?)', ( str(self.trip_id) + self.trip_prefix, route_id,
                        start_time, end_time, (vertices[-1].state.time - vertices[0].state.time ) ))


        for c, v in enumerate(vertices):
            time = datetime.datetime.fromtimestamp(v.state.time)

            self.cursor.execute('INSERT INTO trip_details VALUES (?,?,?,?,?,?,?,?)',
                                            ( str(self.trip_id) + self.trip_prefix, c, v.label, time, v.state.weight,
                                              v.state.dist_walked, v.state.num_transfers,
                                              v.state.trip_id ))
        self.trip_id += 1


    ''' this method will write a very long trip into the database. '''
    def write_error_trip(self, start_time, route_id):
        start_date_time = datetime.datetime.fromtimestamp(start_time)
        end_time = datetime.datetime(2030,12,31)

        self.cursor.execute('INSERT INTO trips VALUES (?,?,?,?,?)', ( str(self.trip_id) + self.trip_prefix, route_id,
                        start_date_time, end_time, (time.mktime(end_time.timetuple()) - start_time ) ))

        self.trip_id += 1


    def __init__(self, graph, route_db_filename, time_step=240, walking_speed=1.2, max_walk=1080, walking_reluctance=2, trip_prefix=''):

        self.trip_prefix = trip_prefix
        self.time_step = time_step

        self.walk_ops = WalkOptions()
        self.walk_ops.walking_speed = walking_speed
        self.walk_ops.max_walk = max_walk
        self.walk_ops.walking_reluctance = walking_reluctance

        self.graph = graph
        self.conn = sqlite3.connect(route_db_filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.trip_id = 0
        self.create_db_tables()

        self.process()
