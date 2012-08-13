#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 21.10.2010
# Gertz Gutsche Rümenapp Gbr

import psycopg2
import datetime
import sys
import math
import thread
import time

from termcolor import colored

from graphserver.graphdb import GraphDatabase
from graphserver.core import State

from graphserver_tools.utils.utils import distance, string_to_datetime
from graphserver_tools.utils import utf8csv


def read_points_0(f, conn):
    """Load points from csv file into database"""
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS cal_points CASCADE')
    sql = '''
    CREATE OR REPLACE VIEW public.cal_points_0(
    id,
    lat,
    lon,
    name)
    AS
      SELECT row_number() OVER(
      ORDER BY origins.name) ::integer AS id,
               origins.lat,
               origins.lon,
               origins.name
      FROM origins
      UNION ALL
      SELECT row_number() OVER(
      ORDER BY destinations.name) ::integer + 1000000 AS id,
               destinations.lat,
               destinations.lon,
               destinations.name
      FROM destinations;
       '''
    cursor.execute(sql)
    cursor.close()
    conn.commit()



def read_points(f, conn):
    """Load points from csv file into database"""
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS cal_points CASCADE')
    cursor.execute('''CREATE TABLE cal_points ( id INTEGER PRIMARY KEY,
                                            lat REAL NOT NULL,
                                            lon REAL NOT NULL,
                                            name TEXT )''')
    reader = utf8csv.UnicodeReader(open(f))

    header = reader.next()
    id_column = header.index(u'id')
    lat_column = header.index(u'lat')
    lon_column = header.index(u'lon')
    name_column = header.index(u'name')

    points = {}
    for line in reader:
        cursor.execute('INSERT INTO cal_points VALUES (%s,%s,%s,%s)',
                         ( line[id_column], line[lat_column], line[lon_column], line[name_column] ))

    cursor.close()
    conn.commit()


def read_times(f, conn):
    """Load timetables from csv file into database"""
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS cal_times CASCADE')
    cursor.execute('''CREATE TABLE cal_times ( id INTEGER PRIMARY KEY,
                                               start_time TIMESTAMP NOT NULL,
                                               end_time TIMESTAMP NOT NULL,
                                               is_arrival_time BOOLEAN NOT NULL  )''')

    reader = utf8csv.UnicodeReader(open(f))

    header = reader.next()

    id_column = header.index(u'id')
    start_column = header.index(u'start')
    end_column = header.index(u'end')
    is_arrival_column = header.index(u'is_arrival_time')

    for line in reader:
        start_date = string_to_datetime(line[start_column])
        end_date = string_to_datetime(line[end_column])

        if (end_date - start_date).total_seconds() < 1:
            print(colored('WARNING: invalid time window at id: %s' % line[id_column], 'yellow'))

        cursor.execute('INSERT INTO cal_times VALUES (%s,%s,%s,%s)',
                            ( line[id_column], start_date, end_date, line[is_arrival_column] ))

    cursor.close()
    conn.commit()


def read_routes_0(f, conn):
    """Load routes from cal_routes_0_view into cal_routes
    references to points and timetable"""
    cursor = conn.cursor()


    sql = '''
    CREATE OR REPLACE VIEW cal_routes_0 AS

    SELECT
    row_number() OVER(ORDER BY origin.id,d.id):: integer AS id,
    origin.id AS origin,
    d.id AS destination,
    destinations.time_id,
    FALSE AS done
    FROM
    (SELECT cal_points_0.id FROM cal_points_0 WHERE cal_points_0.id < 1000000) AS origin,
    (SELECT cal_points_0.id,cal_points_0.name FROM cal_points_0 WHERE cal_points_0.id >= 1000000) AS d,
    destinations
    WHERE d.name=destinations.name;
    '''

    cursor.execute(sql)

    #cursor.execute('DROP TABLE IF EXISTS cal_routes_0 CASCADE')
    cursor.execute('''CREATE OR REPLACE TABLE cal_routes_0 ( id INTEGER PRIMARY KEY,
                                                origin INTEGER REFERENCES cal_points,
                                                destination INTEGER REFERENCES cal_points,
                                                time INTEGER REFERENCES cal_times,
                                                done BOOLEAN )''')


    cursor.execute('INSERT INTO cal_routes SELECT * FROM cal_routes_0')
    cursor.execute('CREATE INDEX IDX_time ON cal_routes ( time )')
    cursor.execute('CREATE INDEX IDX_origin ON cal_routes ( origin )')
    cursor.execute('CREATE INDEX IDX_destination ON cal_routes ( destination )')
    cursor.execute('CREATE INDEX IDX_done ON cal_routes ( done )')

    cursor.close()
    conn.commit()


def read_routes(f, conn):
    """Load routes from csv file into database
    references to points and timetable"""
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS cal_routes CASCADE')
    cursor.execute('''CREATE TABLE cal_routes ( id INTEGER PRIMARY KEY,
                                                origin INTEGER REFERENCES cal_points,
                                                destination INTEGER REFERENCES cal_points,
                                                time INTEGER REFERENCES cal_times,
                                                done BOOLEAN )''')

    cursor.execute('CREATE INDEX IDX_time ON cal_routes ( time )')
    cursor.execute('CREATE INDEX IDX_origin ON cal_routes ( origin )')
    cursor.execute('CREATE INDEX IDX_destination ON cal_routes ( destination )')
    cursor.execute('CREATE INDEX IDX_done ON cal_routes ( done )')



    reader = utf8csv.UnicodeReader(open(f))

    header = reader.next()

    id_column = header.index(u'id')
    origin_column = header.index(u'origin')
    dest_column = header.index(u'destination')
    time_id_column = header.index(u'time_id')

    try:
        for i, line in enumerate(reader):

            cursor.execute('INSERT INTO cal_routes VALUES (%s,%s,%s,%s,%s)', ( line[id_column],
                                                                      line[origin_column],
                                                                      line[dest_column],
                                                                      line[time_id_column],
                                                                      False                 ))

    except:
        print(colored("ERROR: time, origin or desitionation could not be found on route %s!" % i, "red"))
        raise
    finally:
        cursor.close()
        conn.commit()

def calc_corresponding_vertices(graph, db_conn_string):

    def closest_vertices(list):
        ''' wrapper function for multithreading'''

        try:
            for i, (id, lat, lon) in enumerate(list):
                closest_vertex(id, lat, lon)

        except psycopg2.OperationalError: # pick up the work if the cursor gets closed due to bad network connection
            closest_vertices(list[i:])


    def closest_vertex(id, lat, lon):
        cv = None
        min_dist = sys.maxint

        for s_id, s_lat, s_lon in stations:
            dist = distance(lat, lon, s_lat, s_lon)

            if dist < min_dist:
                min_dist = dist
                cv = 'sta-' + s_id

        range = 0.05 # might not be the best number

        conn = psycopg2.connect(db_conn_string)
        c = conn.cursor()

        c.execute('''SELECT id, lat, lon FROM osm_nodes WHERE endnode_refs > 1 AND lat > %s AND lat < %s  AND lon > %s AND lon < %s''',
                                                                                            ( lat-range, lat+range, lon-range, lon+range ))
        nodes = [n for n in c]

        for n_id, n_lat, n_lon in nodes:
            dist = distance(lat, lon, n_lat, n_lon)

            if dist < min_dist:
                min_dist = dist
                cv = 'osm-' + n_id

        corres_vertices.append(( id, cv ))
        if not c.closed:
            c.close();
        if not conn.closed:
            conn.close();


    # do the setup
    conn = psycopg2.connect(db_conn_string)
    c = conn.cursor()
    c.execute('SELECT id, lat, lon FROM cal_points')
    points = c.fetchall()

    c.execute('SELECT stop_id, stop_lat, stop_lon FROM gtfs_stops')
    stations = c.fetchall()

    corres_vertices = [] # will contain tuples of points with their corresponding vertices

    # start a few threads for calculating
    num_threads = 32 #  there will actualy be one more
    num_calculations_per_thread = len(points) / num_threads

    for i in range(num_threads):
        thread.start_new_thread( closest_vertices, (points[i*num_calculations_per_thread:(i+1)*num_calculations_per_thread], ))

    # a few points won't be calculted due to integer division
    thread.start_new_thread( closest_vertices, (points[(i+1)*num_calculations_per_thread:], ) )

    # wait till all points are calculated
    finished = False

    while not finished:
        sys.stdout.write('\r%s/%s corresponding points found' % ( len(corres_vertices), len(points) ))
        sys.stdout.flush()

        time.sleep(1.0)

        if set([id for id, lat, lon in points]) == set([id for id, cv in corres_vertices]):
            finished = True


    print('\r%s corresponding points found                  ' % len(points))

    if conn.closed:
        conn = psycopg2.connect(db_conn_string)
    if c.closed:
        c = conn.cursor()

    # write the stuff into the database
    c.execute('DROP TABLE IF EXISTS cal_corres_vertices')
    c.execute('CREATE TABLE cal_corres_vertices ( point_id INTEGER PRIMARY KEY REFERENCES cal_points, vertex_label TEXT NOT NULL ) ')

    for id, cv in set(corres_vertices):
        if not cv:
            print(colored("ERROR: point with id %s cannot be linked into graph!" % id, "red"))
            cv = graph.vertices[0].label
        c.execute('INSERT INTO cal_corres_vertices VALUES (%s,%s)', ( id, cv ))

    conn.commit()
    c.close()

