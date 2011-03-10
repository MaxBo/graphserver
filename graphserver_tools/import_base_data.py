#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 21.10.2010
# Gertz Gutsche Rümenapp Gbr

import sqlite3
import sys
from rtree import Rtree

from graphserver.graphdb import GraphDatabase
from graphserver.ext.gtfs.gtfsdb import GTFSDatabase
from graphserver.core import Street
from graphserver.compiler.gdb_import_osm import gdb_import_osm
from graphserver.compiler.gdb_import_gtfs import gdb_load_gtfsdb
from graphserver.ext.osm.osmdb import osm_to_osmdb, OSMDB
from graphserver.ext.osm.osmfilters import DeleteOrphanNodesFilter

from graphserver_tools.utils.utils import read_config, distance



def create_gs_datbases(osm_xml_filename, osmdb_filename, gtfs_filename, gtfsdb_filename, gsdb_filename):

    osm_to_osmdb( osm_xml_filename, osmdb_filename, False, False )

    osmdb = OSMDB( osmdb_filename )

    gtfsdb = GTFSDatabase( gtfsdb_filename, overwrite=True )
    gtfsdb.load_gtfs( gtfs_filename)


    gdb = GraphDatabase( gsdb_filename, overwrite=False )

    gdb_load_gtfsdb( gdb, 1, gtfsdb, gdb.get_cursor())
    gdb_import_osm(gdb, osmdb, 'osm', {}, None);


def link_osm_gtfs(gtfsdb_file, osmdb_file, gdb_file, max_link_dist=150):

    osm_conn = sqlite3.connect(osmdb_file)
    osm_cursor = osm_conn.cursor()

    gdb = GraphDatabase(gdb_file)

    gtfsdb = GTFSDatabase(gtfsdb_file)
    stations = gtfsdb.stops()

    for i, (s_label, s_name, s_lat, s_lon) in enumerate(stations):
        j = False

        range = 0.01 # might not be the best number
        osm_cursor.execute('''SELECT id, lat, lon FROM nodes WHERE endnode_refs > 1
                                                                AND lat > ? AND lat < ?
                                                                AND lon > ? AND lon < ?''',
                                        ( s_lat-range, s_lat+range, s_lon-range, s_lon+range ))
        nodes = osm_cursor.fetchall()
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
            print('\tWARNING: failed linking %s! (%s, %s)' % (s_label, s_lat, s_lon))


def add_missing_stops(gtfsdb_filename, gsdb_filename):
    gtfsdb_conn = sqlite3.connect(gtfsdb_filename)
    gsdb_conn = sqlite3.connect(gsdb_filename)

    gtfsdb_c = gtfsdb_conn.cursor()
    gsdb_c = gsdb_conn.cursor()

    gtfsdb_c.execute('SELECT stop_id FROM stops')

    for s in gtfsdb_c:
        stop_label = 'sta-' + s[0]
        gsdb_c.execute('SELECT * FROM vertices WHERE label=?', (stop_label, ))

        if len(gsdb_c.fetchall()) == 0:
            gsdb_c.execute('INSERT INTO vertices VALUES (?)', (stop_label, ))

    gtfsdb_conn.commit()
    gsdb_conn.commit()


def delete_orphan_nodes(osmdb_filename):

    db = OSMDB(osmdb_filename, rtree_index=False)
    filter = DeleteOrphanNodesFilter()

    filter.run(db, *[])

    # reindex the database
    db.index = Rtree(db.dbname)
    db.index_endnodes()