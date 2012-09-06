#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 21.10.2010
# Gertz Gutsche Rümenapp Gbr

import sys
import psycopg2
from rtree import Rtree
import multiprocessing
import time

from termcolor import colored, cprint

from graphserver.graphdb import GraphDatabase
from graphserver.ext.gtfs.gtfsdb import GTFSDatabase
from graphserver.core import Street
from graphserver.compiler.gdb_import_osm import gdb_import_osm
from graphserver.compiler.gdb_import_gtfs import gdb_load_gtfsdb
from graphserver.ext.osm.osmdb import osm_to_osmdb, OSMDB
from graphserver.ext.osm.osmfilters import DeleteOrphanNodesFilter

from graphserver_tools.utils.utils import read_config, distance



def create_gs_datbases(osm_xml_filename, gtfs_filename, db_conn_string):
    """Load the OSM and gtfs data from the files into the database"""

    def importOsmWrapper(osm_xml_filename, db_conn_string):

        gdb = GraphDatabase( db_conn_string, overwrite=True )

        osmdb = osm_to_osmdb( osm_xml_filename, db_conn_string )
        gdb_import_osm(gdb, osmdb, 'osm', {}, None)
        c = gdb.get_cursor()
        #try: c.execute('''SELECT DropGeometryColumn('osm_nodes','geom')''')
        #except: pass 
        c.execute('''SELECT AddGeometryColumn('osm_nodes', 'geom', 4326, 'POINT', 2); 
                     UPDATE osm_nodes SET geom = st_setsrid(st_makepoint(lon,lat),4326);
                     CREATE INDEX osm_nodes_geom_idx ON osm_nodes USING gist(geom);
                     ANALYZE osm_nodes;''')
        c.commit()
        c.close()


    def importGtfsWrapper(gtfs_filename, db_conn_string):

        gdb = GraphDatabase( db_conn_string, overwrite=False )

        gtfsdb = GTFSDatabase( db_conn_string, overwrite=True )
        gtfsdb.load_gtfs( gtfs_filename )

        gdb_load_gtfsdb( gdb, 1, gtfsdb, gdb.get_cursor())
        c = gdb.get_cursor()
        #try: c.execute('''SELECT DropGeometryColumn('gtfs_stops','geom')''')
        #except: pass        
        c.execute('''SELECT AddGeometryColumn('gtfs_stops', 'geom', 4326, 'POINT', 2);
                     UPDATE gtfs_stops SET geom = st_setsrid(st_makepoint(stop_lon,stop_lat),4326);
                     CREATE INDEX gtfs_stops_geom_idx ON gtfs_stops USING gist(geom);
                     ANALYZE gtfs_stops;''')
        c.commit()
        c.close()


    osm_process = multiprocessing.Process(target=importOsmWrapper, args=(osm_xml_filename, db_conn_string))
    osm_process.start()

    time.sleep(1)

    gtfs_process = multiprocessing.Process(target=importGtfsWrapper, args=(gtfs_filename, db_conn_string))
    gtfs_process.start()

    osm_process.join()
    gtfs_process.join()


def link_osm_gtfs(db_conn_string, max_link_dist=150):
    """Link the OSM and the transit-feed
    
    add edges (to table graph_edges) between a stop-node and an osm-node within a defined range (both directions)
        
    """

    conn = psycopg2.connect(db_conn_string)
    cursor = conn.cursor()
    c = conn.cursor()
    gdb = GraphDatabase(db_conn_string)

    #range = 5000 #osm nodes corresponding to the gtfs node are searched for within this range (in meters)
    cursor.execute('SELECT COUNT(*) FROM gtfs_stops')
    stops_nr = cursor.fetchone()[0]
    cursor.execute('SELECT stop_id, geom FROM gtfs_stops')
    stops = cursor.fetchone()
    i=0
    while stops:
        s_label, g_geom = stops

        c.execute('''WITH index_query AS(
                     SELECT id AS n_label, 
                            st_distance(st_transform(o.geom, 31467), st_transform(%s, 31467)) AS distance
                     FROM osm_nodes o
                     WHERE endnode_refs > 1
                     ORDER BY geom <-> %s limit 1000)
                     SELECT * FROM index_query 
                     ORDER BY distance''', (g_geom, g_geom))
        found_one = False
        osm_link = c.fetchone()
        i+=1
        while osm_link:
            n_label, distance = osm_link
            if distance <= max_link_dist:
                gdb.add_edge('sta-'+s_label, 'osm-'+n_label, Street('gtfs-osm link', distance))
                gdb.add_edge('osm-'+n_label, 'sta-'+s_label, Street('gtfs-osm link', distance))
                found_one = True
            elif not found_one: #add nearest node if no node was found within max. distance
                gdb.add_edge('sta-'+s_label, 'osm-'+n_label, Street('gtfs-osm link', distance))
                gdb.add_edge('osm-'+n_label, 'sta-'+s_label, Street('gtfs-osm link', distance))
                found_one = True
                break
            else: break
            osm_link = c.fetchone()
        if not found_one: 
            print(colored('WARNING: failed linking %s!)' % (s_label), 'yellow'))
            i-=1
        sys.stdout.write('\r%s/%s gtfs stops linked' % ( i, stops_nr ))
        sys.stdout.flush()
        stops = cursor.fetchone()
    print
    gdb.commit()
    conn.commit()
    cursor.close()
    c.close()


def add_missing_stops(db_conn_string):
    conn = psycopg2.connect(db_conn_string)
    cursor = conn.cursor()

    cursor.execute('SELECT stop_id FROM gtfs_stops')

    for s in cursor.fetchall():
        stop_label = 'sta-' + s[0]
        cursor.execute('SELECT * FROM graph_vertices WHERE label=%s', (stop_label, ))

        if len(cursor.fetchall()) == 0:
            cursor.execute('INSERT INTO graph_vertices VALUES (%s)', (stop_label, ))

    conn.commit()


def delete_orphan_nodes(db_conn_string):

    db = OSMDB(db_conn_string, rtree_index=False)
    filter = DeleteOrphanNodesFilter()

    filter.run(db, *[])

    # reindex the database
    db.index = Rtree()
    db.index_endnodes()
