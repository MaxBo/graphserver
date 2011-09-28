#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 21.10.2010
# Gertz Gutsche Rümenapp Gbr

import sys
import psycopg2
from rtree import Rtree

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
    osmdb = osm_to_osmdb( osm_xml_filename, db_conn_string )

    gtfsdb = GTFSDatabase( db_conn_string, overwrite=True )
    gtfsdb.load_gtfs( gtfs_filename )

    gdb = GraphDatabase( db_conn_string, overwrite=True )

    gdb_load_gtfsdb( gdb, 1, gtfsdb, gdb.get_cursor())
    gdb_import_osm(gdb, osmdb, 'osm', {}, None);


def link_osm_gtfs(db_conn_string, max_link_dist=150):

    conn = psycopg2.connect(db_conn_string)
    cursor = conn.cursor()

    gdb = GraphDatabase(db_conn_string)

    cursor.execute('SELECT stop_id, stop_lat, stop_lon FROM gtfs_stops')
    for i, (s_label, s_lat, s_lon) in enumerate(cursor.fetchall()):
        j = False

        range = 0.05 # might not be the best number
        cursor.execute('''SELECT id, lat, lon FROM osm_nodes WHERE endnode_refs > 1 AND lat > %s AND lat < %s AND lon > %s AND lon < %s''', ( s_lat-range, s_lat+range, s_lon-range, s_lon+range ))
        nodes = cursor.fetchall()
        dists = []

        for n_label, n_lat, n_lon in nodes:
            dists.append( distance(s_lat, s_lon, n_lat, n_lon) )

        for d in dists:
            if d < max_link_dist:
                j = True

                n_label, n_lat, n_lon = nodes[dists.index(d)]

                gdb.add_edge('sta-'+s_label, 'osm-'+n_label, Street('gtfs-osm link', d))
                gdb.add_edge('osm-'+n_label, 'sta-'+s_label, Street('gtfs-osm link', d))


        if not j and dists: # fallback mode
            d = min(dists)

            n_label, n_lat, n_lon = nodes[dists.index(d)]

            gdb.add_edge('sta-'+s_label, 'osm-'+n_label, Street('gtfs-osm link', d))
            gdb.add_edge('osm-'+n_label, 'sta-'+s_label, Street('gtfs-osm link', d))


        if not dists:
            print(colored('WARNING: failed linking %s! (%s, %s)' % (s_label, s_lat, s_lon), 'yellow'))

    gdb.commit()
    conn.commit()
    cursor.close()


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
