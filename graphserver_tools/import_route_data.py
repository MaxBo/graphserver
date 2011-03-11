#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 21.10.2010 - 06.12.2010
# Gertz Gutsche Rümenapp Gbr

import psycopg2
import datetime
import sys
import math
import thread
import time

from graphserver.graphdb import GraphDatabase
from graphserver.core import State

from graphserver_tools.utils.utils import distance, string_to_datetime
from graphserver_tools.utils import utf8csv


def read_points(f, conn):
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS points')
    cursor.execute('''CREATE TABLE points ( id INTEGER PRIMARY KEY,
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
        cursor.execute('INSERT INTO points VALUES (?,?,?,?)',
                         ( line[id_column], line[lat_column], line[lon_column], line[name_column] ))

    cursor.close()
    conn.commit()


def read_times(f, conn):
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS times')
    cursor.execute('''CREATE TABLE times ( id INTEGER PRIMARY KEY,
                                           start TIMESTAMP NOT NULL,
                                           end TIMESTAMP  NOT NULL,
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

        cursor.execute('INSERT INTO times VALUES (?,?,?,?)',
                            ( line[id_column], start_date, end_date, line[is_arrival_column] ))

    cursor.close()
    conn.commit()


def read_routes(f, conn):
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS routes')
    cursor.execute('''CREATE TABLE routes ( id INTEGER PRIMARY KEY,
                                            origin INTEGER REFERENCES points,
                                            destination INTEGER REFERENCES points,
                                            time INTEGER REFERENCES times,
                                            done BOOLEAN )''')
    reader = utf8csv.UnicodeReader(open(f))

    header = reader.next()

    id_column = header.index(u'id')
    origin_column = header.index(u'origin')
    dest_column = header.index(u'destination')
    time_id_column = header.index(u'time_id')

    for line in reader:
        cursor.execute('INSERT INTO routes VALUES (?,?,?,?,?)', ( line[id_column],
                                                                  line[origin_column],
                                                                  line[dest_column],
                                                                  line[time_id_column],
                                                                  0                     ))
    cursor.close()
    conn.commit()

def calc_corresponding_vertices(conn, graph, osmdb, gtfsdb):

    ''' wrapper function for multithreading'''
    def closest_vertices(list):
        for id, lat, lon in list:
            closest_vertex(id, lat, lon)


    def closest_vertex(id, lat, lon):
        cv = None
        min_dist = sys.maxint

        for s_id, s_lat, s_lon in stations:
            dist = distance(lat, lon, s_lat, s_lon)

            if dist < min_dist:
                min_dist = dist
                cv = 'sta-' + s_id

        conn = sqlite3.connect(osmdb)
        c = conn.cursor()

        range = 0.01 # might not be the best number
        c.execute('''SELECT id, lat, lon FROM nodes WHERE endnode_refs > 1 AND lat > ? AND lat < ? AND lon > ? AND lon < ?''',
                                                            ( lat-range, lat+range, lon-range, lon+range ))
        nodes = c.fetchall()
        c.close()

        for n_id, n_lat, n_lon in nodes:
            dist = distance(lat, lon, n_lat, n_lon)

            if dist < min_dist:
                min_dist = dist
                cv = 'osm-' + n_id

        corres_vertices.append(( id, cv ))


    # do the setup
    cursor = conn.cursor()
    points = cursor.execute('SELECT id, lat, lon FROM points').fetchall()

    conn = sqlite3.connect(gtfsdb)
    c = conn.cursor()
    stations = c.execute('SELECT stop_id, stop_lat, stop_lon FROM stops').fetchall()
    c.close()

    corres_vertices = [] # will contain tuples of points with their corresponding vertices

    # start a few threads for calculating
    num_threads = 32 #  there will actualy be one more
    num_calculations_per_thread = len(points) / num_threads

    for i in range(num_threads):
        thread.start_new_thread( closest_vertices, (points[i*num_calculations_per_thread:(i+1)*num_calculations_per_thread], ))

    # a few points won't be calculted due to integer division
    thread.start_new_thread( closest_vertices, (points[(i+1)*num_calculations_per_thread:], ) )

    # wait till all threads are finished
    while len(corres_vertices) != len(points):
        sys.stdout.write('\r%s/%s corresponding points found' % ( len(corres_vertices), len(points) ))
        sys.stdout.flush()

        time.sleep(1.0)

    print('\r%s corresponding points found                  ' % len(points))

    # write the stuff into the database
    cursor.execute('DROP TABLE IF EXISTS corres_vertices')
    cursor.execute('''CREATE TABLE corres_vertices ( point_id INTEGER PRIMARY KEY REFERENCES points,
                                                     vertex_label TEXT ) ''')

    for id, cv in corres_vertices:
        cursor.execute('INSERT INTO corres_vertices VALUES (?,?)', ( id, cv ))

    cursor.close()
    conn.commit()
