#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 21.10.2010 - 06.12.2010
# Gertz Gutsche Rümenapp Gbr


import sqlite3
import datetime
import sys
import math

from graphserver.graphdb import GraphDatabase
from graphserver.core import State

from graphserver_tools import utf8csv
from graphserver_tools.utils import distance


def read_points(f, cursor):
    cursor.execute('CREATE TABLE points ( id INTEGER PRIMARY KEY, lat REAL, lon REAL, name TEXT )')
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


def read_times(f, cursor):
    cursor.execute('''CREATE TABLE times ( id INTEGER PRIMARY KEY,
                                           start TIMESTAMP,
                                           end TIMESTAMP,
                                           is_arrival_time INTEGER )''')
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


def read_routes(f, cursor):
    cursor.execute('''CREATE TABLE routes ( id INTEGER PRIMARY KEY,
                                            origin INTEGER,
                                            destination INTEGER,
                                            time TEXT ,
                                            done INTEGER )''')
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
                                                                0 ))

def calc_corresponding_vertices(cursor, graph, osmdb, gtfsdb):
    cursor.execute('CREATE TABLE corres_vertices ( point_id INTEGER, vertex_label TEXT )')

    points = cursor.execute('SELECT id, lat, lon FROM points').fetchall()

    for i, (id, lat, lon) in enumerate(points):
        sys.stdout.write('\r%s/%s corresponding points found' % ( i, len(points) ))
        sys.stdout.flush()

        cv = closest_vertex(lat, lon, gtfsdb, osmdb, graph)
        cursor.execute('INSERT INTO corres_vertices VALUES (?,?)', ( id, cv ))
    print('\n')


def string_to_datetime(s):
    # format: DD:MM:YYYY:HH:MM
    sl = [int(x) for x in s.split(':')]
    return datetime.datetime(sl[2],sl[1], sl[0], sl[3], sl[4])


def closest_vertex(lat, lon, gtfsdb, osmdb, graph):
    cv = None
    min_dist = sys.maxint

    conn = sqlite3.connect(gtfsdb)
    c = conn.cursor()

    c.execute('SELECT stop_id, stop_lat, stop_lon FROM stops')

    for s_id, s_lat, s_lon in c:
        dist = distance(lat, lon, s_lat, s_lon)

        if dist < min_dist:
            min_dist = dist
            cv = 'sta-' + s_id

    c.close()


    conn = sqlite3.connect(osmdb)
    c = conn.cursor()

    range = 0.05 # might not be the best number
    c.execute('''SELECT id, lat, lon FROM nodes WHERE endnode_refs > 1 AND lat > ? AND lat < ?
                                                                       AND lon > ? AND lon < ?''',
                                                    ( lat-range, lat+range, lon-range, lon+range ))
    for n_id, n_lat, n_lon in c:
        n_id = 'osm-' + n_id

        dist = distance(lat, lon, n_lat, n_lon)

        if dist < min_dist:
            min_dist = dist
            cv = n_id

    c.close()
    return cv



def main(points_filename, routes_filename, times_filename, gdb_filename, gtfsdb_filename, osmdb_filename, routingdb_file):

    g = GraphDatabase(gdb_filename).incarnate()

    conn = sqlite3.connect(routingdb_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    read_times(times_filename, cursor)
    read_points(points_filename, cursor)
    read_routes(routes_filename, cursor)

    calc_corresponding_vertices(cursor, g ,osmdb_filename, gtfsdb_filename)

    g.destroy()

    conn.commit()