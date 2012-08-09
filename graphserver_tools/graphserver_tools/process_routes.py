#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 24.10.2010
# Gertz Gutsche Rümenapp Gbr

import datetime
import time
import sys
import psycopg2
import thread

from graphserver.core import WalkOptions, State
from graphserver.graphdb import GraphDatabase

from graphserver_tools.utils import utils


class Proccessing():
    def get_gs_vertex(self, point_id):
        """getter for vertex label"""
        self.cursor.execute('SELECT vertex_label FROM cal_corres_vertices WHERE point_id=%s', ( point_id, ))
        return self.cursor.fetchone()[0]


    def get_point(self, vertex_label):
        """getter for point id"""
        self.cursor.execute('SELECT point_id FROM cal_corres_vertices WHERE vertex_label=%s', ( vertex_label, ))
        return [x[0] for x in self.cursor][0]


    def prepare_times(self, start_time, end_time):
        """Return a descending list of times between start_time and end_time at a distance of time_steps (defined in config file)"""
        times = []

        start = time.mktime(start_time.timetuple())
        end = time.mktime(end_time.timetuple())

        t = end
        while t >= start:
            times.append(t)
            t -= self.time_step

        return times


    def get_route_dict(self):
        """Read the first row of the routes (table cal_routes) that wasn't processed yet (done == false).
        
        Return dictionary with the times and the vertex labels of the destination and (multiple) origins 
        if end point of routes (arrival == true).
        
        Return dictionary with the times and the vertex labels of the origin and (multiple) destinations 
        if starting point of routes (arrival == false).
        
        Set row that was processed to "done"
        
        """
        self.cursor.execute('SELECT origin, destination, time FROM cal_routes WHERE NOT done LIMIT 1')
        row = self.cursor.fetchone()
        if row:
            origin, destination, time = row
        else: # there are no routes to Compute
            return None

        self.cursor.execute('SELECT start_time, end_time, is_arrival_time FROM cal_times WHERE id=%s', ( time, ))
        start_time, end_time, is_arrival = self.cursor.fetchone()

        if is_arrival:
            return self.get_retro_dict(destination, time, start_time, end_time)
        else:
            return self.get_dict(origin, time, start_time, end_time)


    def get_retro_dict(self, destination, time, start_time, end_time):
        self.cursor.execute('''SELECT id, origin FROM cal_routes WHERE destination=%s AND time=%s''', ( destination, time ))
        origins = list(self.cursor.fetchall())

        self.cursor.execute('UPDATE cal_routes SET done=%s WHERE destination=%s AND time=%s', ( True, destination, time ))
        self.conn.commit()

        return { 'destination':self.get_gs_vertex(destination), 'times':self.prepare_times(start_time, end_time),
                 'arrival':True,
                 'origins':[ ( self.get_gs_vertex(orig[1]), orig[0] ) for orig in origins ] }


    def get_dict(self, origin, time, start_time, end_time):
        self.cursor.execute('''SELECT id, destination FROM cal_routes WHERE origin=%s AND time=%s''', ( origin, time ))
        destinations = list(self.cursor.fetchall())

        self.cursor.execute('UPDATE cal_routes SET done=%s WHERE origin=%s AND time=%s', ( True, origin, time ))
        self.conn.commit()

        return { 'origin':self.get_gs_vertex(origin), 'times':self.prepare_times(start_time, end_time),
                 'arrival':False,
                 'destinations':[ ( self.get_gs_vertex(dest[1]), dest[0] ) for dest in destinations ] }


    def process_paths(self, routes):
        """Calculate shortest paths from origin
        Write results into the tables cal_path and cal_paths and cal_paths_details""" 
        for t in routes['times']:
            s = State(1, t)

            # build the shortest path tree at time 't'
            try:
                if len(routes['destinations']) > 1:
                    spt = self.graph.shortest_path_tree(routes['origin'], None, s, self.walk_ops, self.maxtime)
                else:
                    spt = self.graph.shortest_path_tree(routes['origin'],routes['destinations'][0][0], s, self.walk_ops, self.maxtime) # faster but only ONE destination
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


    def process_retro_paths(self, routes):
        """Calculate shortest paths to destination
        Write results into the tables cal_path and cal_paths and cal_paths_details"""
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


    def run(self):
        '''
        method for processing (calculating shortest paths) all routes stored inside the databases
        associated with this object.
        [only routes with the processed flag not set will be processed]
        '''
        routes = self.get_route_dict()
        while ( routes ):
            if routes['arrival']:
                self.process_retro_paths(routes)
            else:
                self.process_paths(routes)

            routes = self.get_route_dict()
            

    def write_retro_trip(self, vertices, route_id):
        ''' in retro_paths the walking distance is counted in the wrong direction.
            this method corrects this.
        '''

        # now done in write_results

        self.write_trip(vertices, route_id)

    def write_trip(self, vertices, route_id):
        """Write passed routes and calculation id into database"""
        current_trip_id = str(self.trip_id)
        self.trip_id += 1

        start_time = datetime.datetime.fromtimestamp(vertices[0].state.time)
        end_time = datetime.datetime.fromtimestamp(vertices[-1].state.time)
        self.cursor.execute('INSERT INTO cal_paths VALUES (%s,%s,%s,%s,%s)', ( self.trip_prefix + current_trip_id, route_id, start_time, end_time, (vertices[-1].state.time - vertices[0].state.time ) ))

        for c, v in enumerate(vertices):
            time = datetime.datetime.fromtimestamp(v.state.time)

            self.cursor.execute('INSERT INTO cal_paths_details VALUES (%s,%s,%s,%s,%s,%s,%s,%s)', ( self.trip_prefix + current_trip_id, c, v.label, time, v.state.weight, v.state.dist_walked, v.state.num_transfers, v.state.trip_id ))
        if not self.trips_calculated % 1000:
            self.conn.commit()
            self.logfile.write('%s routes calculated by %s, last route: %s \n' %(self.trips_calculated, self.trip_prefix, route_id))
            self.logfile.flush()
        self.trips_calculated += 1


    def write_error_trip(self, start_time, route_id):
        """Write a long trip (representing "unreachable"?) into database"""
        current_trip_id = str(self.trip_id)
        self.trip_id += 1

        start_date_time = datetime.datetime.fromtimestamp(start_time)
        end_time = datetime.datetime(2030,12,31)

        self.cursor.execute('INSERT INTO cal_paths VALUES (%s,%s,%s,%s,%s)', (self.trip_prefix + current_trip_id, route_id, start_date_time, end_time, (time.mktime(end_time.timetuple()) - start_time ) ))


    def __init__(self, graph, db_connection_string, maxtime = 2000000000, time_step=240, walking_speed=1.2, max_walk=1080, walking_reluctance=2, trip_prefix='', logfile = None):

        self.trip_prefix = trip_prefix
        self.time_step = time_step
        self.maxtime = maxtime

        self.walk_ops = WalkOptions()
        self.walk_ops.walking_speed = walking_speed
        self.walk_ops.max_walk = max_walk
        self.walk_ops.walking_reluctance = walking_reluctance

        self.graph = graph
        self.conn = psycopg2.connect(db_connection_string)
        self.cursor = self.conn.cursor()
        self.trip_id = 0
        self.trips_calculated = 0
        self.logfile = logfile

        self.get_old_trip_id()
        self.run()
        
    def get_old_trip_id(self):
        """continue the trip_id with the id of the last calculation to prevent key conflicts"""
        
        qid = '%'+self.trip_prefix+'%'
        self.cursor.execute('SELECT id FROM cal_paths WHERE id LIKE %(tid)s', {'tid': qid})
        trip_ids = self.cursor.fetchall()
        if trip_ids:
            maxid = 0
            for i, in trip_ids:
                i = i.replace(self.trip_prefix, "")
                if int(i) > maxid:
                    maxid = int(i)
            self.trip_id = maxid
        self.trip_id = self.trip_id + 1        



    def __del__(self):
        self.walk_ops.destroy()
        self.cursor.close()
        self.conn.commit()
        self.graph.destroy()


def create_db_tables(connection, recreate=False):
    """Create the tables cal_paths and cal_paths_details which are needed 
    to store the results of the calculation of the shortest paths
    Overwrite existing tables if argument recreate is set to True"""
    cursor = connection.cursor()

    cursor.execute("select tablename from pg_tables where schemaname='public'" )
    tables = cursor.fetchall()
    
    if ( 'cal_routes', ) not in tables:   #added, was missing for cal_paths reference
        cursor.execute('''CREATE TABLE cal_routes ( id INTEGER PRIMARY KEY,
                                                origin INTEGER REFERENCES cal_points,
                                                destination INTEGER REFERENCES cal_points,
                                                time INTEGER REFERENCES cal_times,
                                                done BOOLEAN )''')
    


    if ( 'cal_paths', ) not in tables or recreate:

        if recreate:
            cursor.execute('DROP TABLE IF EXISTS cal_paths CASCADE')

        cursor.execute('''CREATE TABLE cal_paths ( id TEXT PRIMARY KEY,
                                               route_id INTEGER REFERENCES cal_routes,
                                               start_time TIMESTAMP NOT NULL,
                                               end_time TIMESTAMP NOT NULL,
                                               total_time INTEGER NOT NULL )''')

        cursor.execute('UPDATE public.cal_routes SET done = FALSE;')


    if ( 'cal_paths_details', ) not in tables or recreate:

        if recreate:
            cursor.execute('DROP TABLE IF EXISTS cal_paths_details CASCADE')

        cursor.execute('''CREATE TABLE cal_paths_details ( path_id TEXT REFERENCES cal_paths,
                                                           counter INTEGER NOT NULL,
                                                           label TEXT NOT NULL,
                                                           time TIMESTAMP NOT NULL,
                                                           weight INTEGER NOT NULL,
                                                           dist_walked REAL NOT NULL,
                                                           num_transfers INTEGER NOT NULL,
                                                           gtfs_trip_id TEXT,
                                                        UNIQUE (path_id, counter)) ''')

    connection.commit()
    cursor.close()


def print_status(connection, logfile=None):

    def calc_calculation_time(routes_previously_wating, routes_waiting, all_routes, time_finished):
        if routes_previously_wating != routes_waiting:

            if (not routes_previously_wating) or (all_routes - routes_previously_wating == 0):
                return None, routes_waiting

            routes_processed = all_routes - routes_previously_wating
            routes_previously_wating = routes_waiting

            routes_per_second = (time.time() - time_started) / routes_processed
            time_finished = (all_routes - routes_processed) * routes_per_second

        return time_finished, routes_previously_wating


    time_started = time.time()
    time_finished = None
    routes_previously_wating = None
    routes_waiting = None

    cursor = connection.cursor()
    cursor.execute('SELECT count(*) FROM cal_routes')
    all_routes = cursor.fetchone()[0]

    finished = False
    while not finished:
        time.sleep(1.0)
        cursor.execute('SELECT count(*) FROM cal_routes WHERE NOT done')
        routes_waiting = cursor.fetchone()[0]
        if not all_routes:
            finished = True

        else:
            if time_finished:
                text = '\r%s routes waiting to be processed. Finished in about %s              ' % (routes_waiting,
                                                                                                    utils.seconds_time_string(time_finished))
                sys.stdout.write(text)
                sys.stdout.flush()
##                if logfile:
##                    logfile.write(text)
##                    logfile.flush()

            else:
                text = '\r%s routes waiting to be processed. Please wait ...              ' % routes_waiting
                sys.stdout.write(text)
                sys.stdout.flush()
##                if logfile:
##                    logfile.write(text)
##                    logfile.flush()

        time_finished, routes_previously_wating = calc_calculation_time(routes_previously_wating, routes_waiting, all_routes, time_finished)

    connection.close()

    sys.stdout.write('\rThe last routes getting processed. Please wait ...                                                                      \n')
    sys.stdout.flush()
