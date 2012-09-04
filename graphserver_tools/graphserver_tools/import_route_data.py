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

    sql = '''
    CREATE OR REPLACE VIEW public.cal_points_view(
        id,
        lat,
        lon,
        name,
        time_id)
    AS
      SELECT row_number() OVER(
      ORDER BY origins.name) ::integer AS id,
               origins.lat,
               origins.lon,
               origins.name,
               NULL::unknown AS time_id
      FROM origins
      UNION ALL
      SELECT row_number() OVER(
      ORDER BY destinations.name) ::integer + 1000000 AS id,
               destinations.lat,
               destinations.lon,
               destinations.name,
               destinations.time_id
      FROM destinations;;
       '''
    cursor.execute(sql)

    cursor.execute('DROP TABLE IF EXISTS cal_points CASCADE')
    cursor.execute('''CREATE TABLE cal_points ( id INTEGER PRIMARY KEY,
                                            lat REAL NOT NULL,
                                            lon REAL NOT NULL,
                                            name TEXT,
                                            time_id INTEGER )''')
    cursor.execute('INSERT INTO cal_points SELECT * FROM cal_points_view;')
    cursor.execute("""SELECT AddGeometryColumn('cal_points', 'geom', 4326, 'POINT', 2);
                      UPDATE cal_points SET geom = st_setsrid(st_makepoint(lon,lat),4326);
                      CREATE INDEX cal_points_geom_idx ON cal_points USING gist(geom);
                      ANALYZE cal_points;""")

    
    cursor.close()
    conn.commit()



def read_points(f, conn):
    """Load points from csv file into database"""
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS cal_points CASCADE')
    cursor.execute('''CREATE TABLE cal_points ( id INTEGER PRIMARY KEY,
                                            lat REAL NOT NULL,
                                            lon REAL NOT NULL,
                                            name TEXT,
                                            time_id INTEGER  )''')
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
    """Load routes from cal_routes_view into cal_routes
    references to points and timetable"""
    cursor = conn.cursor()


    sql = '''
    CREATE OR REPLACE VIEW cal_routes_view AS

    SELECT
    row_number() OVER(ORDER BY origin.id,d.id):: integer AS id,
    origin.id AS origin,
    d.id AS destination,
    destinations.time_id,
    FALSE AS done
    FROM
    (SELECT c.id FROM cal_points c WHERE c.id < 1000000) AS origin,
    (SELECT c.id,c.name,c.time_id FROM cal_points c WHERE c.id >= 1000000) AS d,
    destinations
    WHERE d.name=destinations.name AND d.time_id = destinations.time_id;
    '''

    cursor.execute(sql)

    cursor.execute('DROP TABLE IF EXISTS cal_routes CASCADE')
    cursor.execute('''CREATE TABLE cal_routes ( id INTEGER PRIMARY KEY,
                                                origin INTEGER REFERENCES cal_points,
                                                destination INTEGER REFERENCES cal_points,
                                                time INTEGER REFERENCES cal_times,
                                                done BOOLEAN )''')


    cursor.execute('INSERT INTO cal_routes SELECT * FROM cal_routes_view')
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
    """Find nearest OSM-Point or gtfs-Stop to the points inside the cal_points table
       Write result (one per point) into table cal_corres_vertices
       
    """
    
    conn = psycopg2.connect(db_conn_string)
    c = conn.cursor()
    c.execute('DROP TABLE IF EXISTS cal_corres_vertices')
    c.execute('CREATE TABLE cal_corres_vertices ( point_id INTEGER PRIMARY KEY REFERENCES cal_points, vertex_label TEXT NOT NULL ) ')
    
    c.execute('SELECT id, geom FROM cal_points')
    points = c.fetchall()
    i = 1
    for point in enumerate(points):
        point_id, geom = point
        #Indexed Preselection of Points (100) from osm and stops by geometry 
        #Look up for nearest point/stop
        c.execute('''WITH index_query AS (
                      SELECT st_distance(st_transform(o.geom, 31467), st_transform('0101000020E61000000567953E193823400007488F3A664940', 31467)) AS distance,
                             id
                      FROM osm_nodes 
                      ORDER BY geom <-> %s limit 100
                     )
                     SELECT * FROM index_query order by distance limit 1;''', (geom))
        near_osm = c.fetchone()
        c.execute('''WITH index_query AS (
                      SELECT st_distance(st_transform(o.geom, 31467), st_transform('0101000020E61000000567953E193823400007488F3A664940', 31467)) AS distance,
                             stop_id
                      FROM gtfs_stops 
                      ORDER BY geom <-> %s limit 100
                     )
                     SELECT * FROM index_query order by distance limit 1;''', (geom))
        near_sta = c.fetchone()
        #write osm or stop
        i+=1
        if not (near_osm and near_sta): 
            print(colored("ERROR: point with id %s cannot be linked into graph!" % point_id, "red"))
            i-=1
        elif near_osm and not near_sta: c.execute('INSERT INTO cal_corres_vertices VALUES (%s,%s)', ( point_id, 'osm-' + near_osm[1] ))
        elif near_sta and not near_osm: c.execute('INSERT INTO cal_corres_vertices VALUES (%s,%s)', ( point_id, 'sta-' + near_sta[1] ))
        elif near_osm[0] < near_sta[0]: c.execute('INSERT INTO cal_corres_vertices VALUES (%s,%s)', ( point_id, 'osm-' + near_osm[1] ))
        else: c.execute('INSERT INTO cal_corres_vertices VALUES (%s,%s)', ( point_id, 'sta-' + near_sta[1] ))        
        sys.stdout.write('\r%s/%s corresponding points found' % ( i, len(points) ))
        sys.stdout.flush()

    conn.commit()
    c.close()
    conn.close()

