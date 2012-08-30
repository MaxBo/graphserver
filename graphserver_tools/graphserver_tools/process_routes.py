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


    def prepare_times(self, start_time, end_time, is_arrival):
        """If is_arrival return a descending list or if its no arrival return an ascending list
        of times between start_time and end_time at a distance of time_steps defined in the config file"""
        times = []

        start = time.mktime(start_time.timetuple())
        end = time.mktime(end_time.timetuple())

        if not is_arrival:
            t = start
            while t <= end:
                times.append(t)
                t += self.time_step
        else:
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
        times = self.prepare_times(start_time, end_time, True)
        is_calc_time = []
        for t in times:
            is_calc_time.append(True)

        return { 'destination':self.get_gs_vertex(destination), 'times': times,
                 'arrival':True,
                 'origins':[ ( self.get_gs_vertex(orig[1]), orig[0], is_calc_time[:]) for orig in origins ] }

    def get_dict(self, origin, time, start_time, end_time):
        self.cursor.execute('''SELECT id, destination FROM cal_routes WHERE origin=%s AND time=%s''', ( origin, time ))
        destinations = list(self.cursor.fetchall())

        self.cursor.execute('UPDATE cal_routes SET done=%s WHERE origin=%s AND time=%s', ( True, origin, time ))
        self.conn.commit()
        times = self.prepare_times(start_time, end_time, False)
        is_calc_time = []
        for t in times:
            is_calc_time.append(True)

        return { 'origin':self.get_gs_vertex(origin), 'times': times,
                 'arrival':False,
                 'destinations':[ ( self.get_gs_vertex(dest[1]), dest[0], is_calc_time[:] ) for dest in destinations ] }


    def process_paths(self, routes):
        """Calculate shortest paths from one origin to various destinations
        Write results into the tables cal_path and cal_paths and cal_paths_details"""
        for time_index, t in enumerate(routes['times']):
            s = State(1, t)

            # build the shortest path tree at time 't'
            try:
                if len(routes['destinations']) > 1:
                    spt = self.graph.shortest_path_tree(routes['origin'], None, s, self.walk_ops, weightlimit = self.weightlimit)
                else:
                    spt = self.graph.shortest_path_tree(routes['origin'],routes['destinations'][0][0], s, self.walk_ops, weightlimit = self.weightlimit) # faster but only ONE destination
            except:
                pass

            # extract the actual routes and write them into the database
            del_dest=[]
            for dest in routes['destinations']:
                if self.fast_calc:       #experimental faster calculation
                    if dest[2][time_index]:   #check if vertex is to be calculated at this time step
                        try:
                            vertices, edges = spt.path(dest[0])
                            if not vertices: raise Exception()
                        except:
                            error_trip = True
                            waiting_time = 999999999  #infinite waiting time if not reachable
                        else:                        
                            waiting_time, error_trip = self.get_waiting_time(vertices, False)
                    
                    #for testing: write error trips for inacceptable travel times and duplicate entries if there is a waiting time
                        entries = 1                            
                        for time_index2, t2 in enumerate(routes['times'[:]]):
                            if t <= t2 <= t + waiting_time:
                                entries+=1
                                dest[2][time_index2] = False #set to false, so that it will be ignored at this time step
                                if error_trip: self.write_error_trip(t2, dest[1], False)                              
                        if not error_trip: self.write_trip(vertices, dest[1], waiting_time, entries, False)
                        if t + waiting_time > routes['times'][-1]: del_dest.append(dest)
                else:       #slower calculation, but stable
                    for dest in routes['destinations']:
                        try:
                            vertices, edges = spt.path(dest[0])
                            if not vertices: raise Exception()
                        except: self.write_error_trip(t, dest[1], False)
                        else: 
                            waiting_time, error_trip = self.get_waiting_time(vertices, False)
                            self.write_trip(vertices, dest[1], waiting_time, 1, False)
            for dest in del_dest:
                routes['destinations'].remove(dest) #remove destinations that don't need to be calculated anymore to fasten iteration
                                
            # cleanup
            try:
                spt.destroy()
            except:
                pass


    def process_retro_paths(self, routes):
        """Calculate shortest paths from various origins to one destination
        Write results into the tables cal_path and cal_paths and cal_paths_details"""
        for time_index, t in enumerate(routes['times']):
            s = State(1, t)

            # build the shortest path tree at time 't'
            try:
                if len(routes['origins']) > 1:
                    spt = self.graph.shortest_path_tree_retro(None, routes['destination'], s,self.walk_ops, weightlimit = self.weightlimit)
                else:
                    spt = self.graph.shortest_path_tree_retro(routes['origins'][0][0], routes['destination'], s, self.walk_ops, weightlimit = self.weightlimit)# faster but only ONE destination
            except:
                pass

            # extract the actual routes and write them into the database
            del_orig=[]
            for orig in routes['origins']:
                if self.fast_calc:      #experimental faster calculation
                    if orig[2][time_index]:   #check if vertex is to be calculated at this time step
                        try:
                            vertices, edges = spt.path_retro(orig[0])
                            if not vertices: raise Exception()
                        except:
                            error_trip = True
                            waiting_time = 999999999  #infinite waiting time if not reachable
                        else:                        
                            waiting_time, error_trip = self.get_waiting_time(vertices, True)
                                        
                        entries = 1
                        #write error trips for inacceptable travel times  
                        if waiting_time >= self.time_step: 
                            entries = 0
                            for time_index2, t2 in enumerate(routes['times'[:]]):
                                if t >= t2 >= t - waiting_time:                                                   
                                    entries+=1
                                    orig[2][time_index2] = False #set to false, so that it will be ignored at this time step
                                    if error_trip: self.write_error_trip(t2, orig[1], True)
                            if t - waiting_time < routes['times'][-1]: del_orig.append(orig)       
                        if not error_trip: self.write_trip(vertices, orig[1], waiting_time, entries, True)
                else:           #slower calculation, but stable
                    try:
                        vertices, edges = spt.path_retro(orig[0])
                        if not vertices: raise Exception()
                    except: self.write_error_trip(t, orig[1], True)
                    else: 
                        waiting_time, error_trip = self.get_waiting_time(vertices, True)
                        self.write_trip(vertices, orig[1], waiting_time, 1, True)
            for orig in del_orig:
                routes['origins'].remove(orig) #remove origins that don't need to be calculated anymore to fasten iteration
                
                            
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
            
    def get_waiting_time(self, vertices, is_arrival=False):
        """look, if there is a waiting time at the first transit stop (for departure-time search)
        of a waiting time at the last transit stop before going home (for arrival-time search)"""
        travel_time = vertices[-1].state.time - vertices[0].state.time
        if travel_time > (self.weightlimit) and self.fast_calc: # and vertices[0].state.dist_walked < self.walk_ops.max_walk / 2:
            return 999999999, True          #if traveltime is not acceptable return "infinite" waiting time


        weight_in_last_row = None
        num_transfers_in_last_row = None
        waiting_time = 0

        for c, v in enumerate(vertices):
            if is_arrival:
                # last transit stop for arrival-time search
                if v.state.num_transfers == 0:
                    if num_transfers_in_last_row == 1:
                        waiting_time = weight_in_last_row - v.state.weight - 1
                        break
            else:
                # first transit stop for departure-time search
                if v.state.num_transfers == 1:
                    if num_transfers_in_last_row == 0:
                        waiting_time = v.state.weight - weight_in_last_row -1
                        break

            weight_in_last_row, num_transfers_in_last_row = v.state.weight, v.state.num_transfers        
        return waiting_time, False


    def write_trip(self, vertices, route_id, waiting_time, entries, is_arrival=False):
        """Write passed routes and calculation id into database, 
        the waiting time will be substracted from the arrival time"""
        
        start_time = datetime.datetime.fromtimestamp(vertices[0].state.time)
        end_time = datetime.datetime.fromtimestamp(vertices[-1].state.time)
        travel_time = vertices[-1].state.time - vertices[0].state.time
        # reduce travel_time by waiting time
        travel_time -= waiting_time
        if is_arrival:
        # actual arriving time is earlier
            end_time -= datetime.timedelta(0,waiting_time)
        else:
        # can depart some minutes later
            start_time += datetime.timedelta(0,waiting_time)

        for i in range(entries):
            current_trip_id = str(self.trip_id) 
            self.trip_id += 1
            self.cursor.execute('INSERT INTO cal_paths VALUES (%s,%s,%s,%s,%s)', ( self.trip_prefix + current_trip_id, route_id, start_time, end_time, travel_time ))

        if self.write_cal_paths_details:
            for c, v in enumerate(vertices):
                time = datetime.datetime.fromtimestamp(v.state.time)
                self.cursor.execute('INSERT INTO cal_paths_details VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',\
                                    ( self.trip_prefix + current_trip_id, c, v.label, time,
                                      v.state.weight, v.state.dist_walked, v.state.num_transfers, v.state.trip_id ))


        if not self.trips_calculated % 1000:
            self.conn.commit()
            self.logfile.write('%s routes calculated by %s, last route: %s \n' %(self.trips_calculated, self.trip_prefix, route_id))
            self.logfile.flush()
        self.trips_calculated += entries

    def write_error_trip(self, start_end_time, route_id, is_arrival = False):
        """Write a long trip representing "unreachable" respectively "inacceptable" into database"""
        current_trip_id = str(self.trip_id)
        self.trip_id += 1
        if is_arrival:
            end_date_time = datetime.datetime.fromtimestamp(start_end_time)
            start_time = datetime.datetime(1985,12,31)
            self.cursor.execute('INSERT INTO cal_paths VALUES (%s,%s,%s,%s,%s)', (self.trip_prefix + current_trip_id, route_id, start_time, end_date_time, (start_end_time - time.mktime(start_time.timetuple()))))
        else:    
            start_date_time = datetime.datetime.fromtimestamp(start_end_time)
            end_time = datetime.datetime(2030,12,31)
            self.cursor.execute('INSERT INTO cal_paths VALUES (%s,%s,%s,%s,%s)', (self.trip_prefix + current_trip_id, route_id, start_date_time, end_time, (time.mktime(end_time.timetuple()) - start_end_time ) ))


    def __init__(self, graph, db_connection_string, time_step=240, walking_speed=1.2, max_walk=1080, walking_reluctance=2, trip_prefix='', logfile = None, write_cal_paths_details=False, max_travel_time = 25200,fast_calc=False):

        self.trip_prefix = trip_prefix
        self.time_step = time_step
        self.write_cal_paths_details = write_cal_paths_details
        self.weightlimit = max_travel_time + int(max_walk / walking_speed * walking_reluctance/2) + 1000 #walking is penalized with higher weights, +1000 if waiting is penalized
        self.walk_ops = WalkOptions()
        self.walk_ops.walking_speed = walking_speed
        self.walk_ops.max_walk = max_walk
        self.walk_ops.walking_reluctance = walking_reluctance
        self.walk_ops.walking_overage = 0.4
        self.max_travel_time = max_travel_time
        self.graph = graph
        self.conn = psycopg2.connect(db_connection_string)
        self.cursor = self.conn.cursor()
        self.trip_id = 0
        self.trips_calculated = 0
        self.logfile = logfile
        self.fast_calc = fast_calc
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


    cursor.execute('''CREATE OR REPLACE VIEW public.best_time (route_id,
                                                               start_time,
                                                               end_time,
                                                               total_time)
                    AS
                    SELECT a.route_id, a.start_time, a.end_time, a.total_time
                    FROM (
                        SELECT p.route_id, row_number() OVER (PARTITION BY p.route_id ORDER BY p.total_time) AS rownumber,
                        p.start_time,
                        p.end_time,
                        p.total_time
                        FROM cal_paths p
                        ) a
                    WHERE a.rownumber = 1;''')
    sql = '''
    CREATE OR REPLACE VIEW Ergebnis AS
    SELECT
      o.name AS origin_name,
      d.name AS destination_name,
      bt.start_time,
      bt.end_time,
      bt.total_time
    FROM
      public.origins o
      INNER JOIN public.cal_points_view p_o ON (o.name = p_o.name)
      INNER JOIN public.cal_routes r ON (p_o.id = r.origin)
      INNER JOIN public.best_time bt ON (r.id = bt.route_id)
      INNER JOIN public.cal_points_view p_d ON (r.destination = p_d.id)
      INNER JOIN public.destinations d ON (p_d.name = d.name)
    '''
    #cursor.execute(sql)

    connection.commit()
    cursor.close()


def print_status(connection, time_step, logfile=None):

    def calc_calculation_time(routes_previously_waiting, routes_waiting, all_routes, time_finished):
        if routes_previously_waiting != routes_waiting:

            if (not routes_previously_waiting) or (all_routes - routes_previously_waiting == 0):
                return None, routes_waiting

            routes_processed = all_routes - routes_previously_waiting
            routes_previously_waiting = routes_waiting

            routes_per_second = (time.time() - time_started) / routes_processed
            time_finished = (all_routes - routes_processed) * routes_per_second

        return time_finished, routes_previously_waiting


    time_started = time.time()
    time_finished = None
    routes_previously_waiting = None
    routes_waiting = None

    cursor = connection.cursor()
    cursor.execute('SELECT count(*) FROM cal_routes')
    all_routes = cursor.fetchone()[0]

    finished = False
    while not finished:
        time.sleep(1.0)
        cursor.execute('SELECT count(*) FROM cal_routes WHERE NOT done')        
        routes_waiting = cursor.fetchone()[0]     
           
        if not routes_waiting:
            finished = True

        else:
            if time_finished:
                text = '\r%s routes waiting to be processed. Finished in about %s ' % (routes_waiting, utils.seconds_time_string(time_finished))
                sys.stdout.write(text)
                sys.stdout.flush()
## if logfile:
## logfile.write(text)
## logfile.flush()

            else:
                text = '\r%s routes waiting to be processed. Please wait ...              ' % routes_waiting
                sys.stdout.write(text)
                sys.stdout.flush()
## if logfile:
## logfile.write(text)
## logfile.flush()

        time_finished, routes_previously_waiting = calc_calculation_time(routes_previously_waiting, routes_waiting, all_routes, time_finished)

    connection.close()

    sys.stdout.write('\rThe last routes getting processed. Please wait ...               \n')
    sys.stdout.flush()