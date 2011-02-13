#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 24.10.2010 - 12.11.2010
# Gertz Gutsche Rümenapp Gbr

# Arguments:
# 1 - routing database filename
# 2 - graphserver database filename


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

        try:
            self.cursor.execute('''CREATE TABLE trips ( id INTEGER PRIMARY KEY,
                                                    route_id INTEGER,
                                                    start_time TIMESTAMP,
                                                    end_time TIMESTAMP,
                                                    total_time INTEGER)''')

            self.cursor.execute('''CREATE TABLE trip_details ( trip_id INTEGER,
                                                           counter INTEGER,
                                                           label TEXT,
                                                           time TIMESTAMP,
                                                           weight INTEGER,
                                                           dist_walked REAL,
                                                           num_transfers INTEGER,
                                                           gtfs_trip_id TEXT)''')
        except:
            pass

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


    def process(self):

        w = WalkOptions()
        w.walking_speed = self.walking_speed
        w.max_walk = self.max_walk
        w.walking_reluctance = self.walking_reluctance


        num_all_routes = len(self.cursor.execute('SELECT * FROM routes').fetchall())
        num_proccessed_routes = 0

        routes = self.get_route_dict()
        while ( routes ):
            sys.stdout.write('\r%s/%s shortest paths found' % ( num_proccessed_routes, num_all_routes ))
            sys.stdout.flush()

            if 'destinations' in routes:
                num_proccessed_routes += len(routes['destinations'])
            else:
                num_proccessed_routes += len(routes['origins'])


            if routes['arrival']: # use retro trips
                for t in routes['times']:
                    s = State(1, t)

                    if len(routes['origins']) > 1:
                        spt = self.graph.shortest_path_tree_retro(None, routes['destination'], s, w)
                    else:
                        spt = self.graph.shortest_path_tree_retro(routes['origins'][0][0], routes['destination'], s, w)

                    for orig in routes['origins']:
                        try:
                            vertices, edges = spt.path_retro(orig[0])

                            if not vertices: raise Exception()

                        except:
                            print '\tERROR: there is no path from %s to %s!' % ( orig[0], routes['destination'] )

                        else:
                            self.write_retro_trip(vertices, orig[1])

                    spt.destroy()


            else: # use none retro trips
                for t in routes['times']:
                    s = State(1, t)

                    if len(routes['destinations']) > 1:
                        spt = self.graph.shortest_path_tree(routes['origin'], None, s, w)
                    else:
                        spt = self.graph.shortest_path_tree(routes['origin'],routes['destinations'][0][0], s, w)

                    for dest in routes['destinations']:
                        try:
                            vertices, edges = spt.path(dest[0])

                            if not vertices: raise Exception()

                        except:
                            print '\tERROR: there is no path from %s to %s' % ( routes['origin'], dest[0] )

                        else:
                            self.write_trip(vertices, dest[1])

                    spt.destroy()


            self.conn.commit()
            routes = self.get_route_dict()

        print('\r%s shortest paths found                     ' % num_all_routes )
        w.destroy()


    def write_retro_trip(self, vertices, route_id):
        ''' in retro_paths the walking distance is counted in the wrong direction.
            this method corrects this.
        '''

        total_dist_walked = vertices[0].state.dist_walked

        for v in vertices:
            v.state.dist_walked = total_dist_walked - v.state.dist_walked

        self.write_trip(vertices, route_id)



    def write_trip(self, vertices, route_id):
        start_time = datetime.datetime.fromtimestamp(vertices[0].state.time)
        end_time = datetime.datetime.fromtimestamp(vertices[-1].state.time)

        self.cursor.execute('INSERT INTO trips VALUES (?,?,?,?,?)', ( self.trip_id, route_id,
                        start_time, end_time, (vertices[-1].state.time - vertices[0].state.time ) ))


        for c, v in enumerate(vertices):
            time = datetime.datetime.fromtimestamp(v.state.time)

            self.cursor.execute('INSERT INTO trip_details VALUES (?,?,?,?,?,?,?,?)',
                                            ( self.trip_id, c, v.label, time, v.state.weight,
                                              v.state.dist_walked, v.state.num_transfers,
                                              v.state.trip_id ))
        self.trip_id += 1


    def __init__(self, graph, route_db_filename, time_step=240, walking_speed=1.2, max_walk=1080, walking_reluctance=2):

        self.time_step = time_step
        self.walking_speed = walking_speed
        self.max_walk = max_walk
        self.walking_reluctance = walking_reluctance

        self.graph = graph
        self.conn = sqlite3.connect(route_db_filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.trip_id = 0
        self.create_db_tables()


        self.process()

'''

if __name__ == '__main__':
    from graphserver_tools import utils
    from graphserver.graphdb import GraphDatabase

    g = GraphDatabase(sys.argv[2]).incarnate()

    defaults = { 'time-step':'240', 'max-walk':'11080', 'walking-reluctance':'20', 'walking-speed':'1.2' }

    config = utils.read_config(sys.argv[3], defaults)

    Proccessing(g, sys.argv[1], int(config['time-step']), float(config['walking-speed']), int(config['max-walk']), int(config['walking-reluctance']))

    print 'done proccessing routes'

    g.destroy()

'''