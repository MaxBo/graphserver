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

    def importOsmWrapper(osm_xml_filename, db_conn_string):

        gdb = GraphDatabase( db_conn_string, overwrite=True )

        osmdb = osm_to_osmdb( osm_xml_filename, db_conn_string )
        gdb_import_osm(gdb, osmdb, 'osm', {}, None)

    def importGtfsWrapper(gtfs_filename, db_conn_string):

        gdb = GraphDatabase( db_conn_string, overwrite=False )

        gtfsdb = GTFSDatabase( db_conn_string, overwrite=True )
        gtfsdb.load_gtfs( gtfs_filename )

        gdb_load_gtfsdb( gdb, 1, gtfsdb, gdb.get_cursor())
        c = gdb.get_cursor()
        c.execute("""SELECT AddGeometryColumn('gtfs_stops', 'geom', 4326, 'POINT';""")
        c.execute("""UPDATE gtfs_stops SET geom = st_setsrid(st_makepoint(lon,lat));""")
        c.execute("""CREATE INDEX gtfs_stops_geom_idx ON gtfs_stops USING gist(geom);""")
        c.execute("""ANALYZE gtfs_stops;""")
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

    conn = psycopg2.connect(db_conn_string)
    cursor = conn.cursor()

    gdb = GraphDatabase(db_conn_string)

    cursor.execute("""
    SELECT
      s_label,
      n_label,
      st_distance(st_transform(ogeom, 31467), st_transform(ggeom, 31467)) AS meter
    FROM (
    SELECT
      s_label,
      o.id AS n_label,
      row_number() OVER(PARTITION BY o.id ORDER BY st_distance(o.geom, g.geom)) AS rn,
      o.geom AS ogeom,
      g.geom AS ggeom
    FROM gtfs_stops g LEFT JOIN osm_nodes o ON st_dwithin(g.geom, o.geom, 0.05)
    WHERE o.endnode_refs > 1) a
    WHERE
      meter < {max_link_dist}
      -- fallback, if no link is < 150 m, take closest link
      OR a.rn = 1
    ;""".format(max_link_dist=max_link_dist))
    for i, (s_label, n_label, meter) in enumerate(cursor.fetchall()):
        if n_label is not None:
            gdb.add_edge('sta-'+s_label, 'osm-'+n_label, Street('gtfs-osm link', meter))
            gdb.add_edge('osm-'+n_label, 'sta-'+s_label, Street('gtfs-osm link', meter))
        else:
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
