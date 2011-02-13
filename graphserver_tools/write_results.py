#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 29.10.2010 - 04.11.2010
# Gertz Gutsche Rümenapp Gbr


# Arguments:
#
# 1 - results.csv filename (will be created)
# 2 - result_details.csv filename (will be created)
# 3 - routing db filename
# 4 - osm db filename
# 5 - gtfs db filename


import sqlite3
import datetime

from graphserver_tools import utf8csv



def write_details(conn, filename, gtfsdb_cursor, osmdb_cursor):
    writer = utf8csv.UnicodeWriter(open(filename, 'w'))

    writer.writerow(( u'route_id', u'counter', u'label', u'arrival/departure', u'dist_walked', u'transfers', u'transit_route' ))

    c = conn.cursor()

    routes = set(c.execute('SELECT route_id FROM trips').fetchall())


    for r, in sorted(routes, key=lambda route: route[0]):
        id, route_id = c.execute('''SELECT id, route_id
                          FROM trips
                          WHERE total_time=( SELECT MIN(total_time) FROM trips WHERE route_id=? )
                          AND route_id=?''', ( r, r )).fetchone()

        details = c.execute('''SELECT counter, label, time, dist_walked, num_transfers, gtfs_trip_id
                               FROM trip_details
                               WHERE trip_id=?''', ( id, )).fetchall()


        writer.writerows(humanize_details(route_id, details, gtfsdb_cursor, osmdb_cursor, c))

    c.close()


def write_results(conn, filename):
    writer = utf8csv.UnicodeWriter(open(filename, 'w'))

    writer.writerow(( u'route_id', u'start_time', u'end_time', u'total_time' ))

    c = conn.cursor()

    routes = set(c.execute('SELECT route_id FROM trips').fetchall())


    for r, in sorted(routes, key=lambda route: route[0]):

        fastest_trip = list(c.execute('''SELECT route_id, start_time, end_time, total_time
                                    FROM trips
                                    WHERE total_time=( SELECT MIN(total_time) FROM trips WHERE route_id=? )
                                    AND route_id=?''', ( r, r )).fetchone())

        fastest_trip[3] = '%i:%02i:%02i' % ( fastest_trip[3]/3600, (fastest_trip[3]/60)%60, fastest_trip[3]%60 )


        writer.writerow(fastest_trip)

    c.close()


def get_lat_lon(osmdb_cursor, gs_osm_vertex):
    osmdb_cursor.execute('SELECT lat, lon FROM nodes WHERE id=?', ( gs_osm_vertex[4:], ))
    lat, lon = osmdb_cursor.fetchone()

    return '%.4f, %.4f' % ( lat, lon )


def get_node_name(route_db_cursor, node_label):
    pid, = route_db_cursor.execute('SELECT point_id FROM corres_vertices WHERE vertex_label=?', ( node_label, )).fetchone()

    if not pid: return ''

    return route_db_cursor.execute('SELECT name FROM points WHERE id=?', ( pid, )).fetchone()[0]


def get_station_name(gtfsdb_cursor, gs_sta_vertex):
    gtfsdb_cursor.execute('SELECT stop_name FROM stops WHERE stop_id=?', ( gs_sta_vertex[4:], ))

    return gtfsdb_cursor.next()[0]


def get_route_id(gtfsdb_cursor, gtfs_trip_id):
    gtfsdb_cursor.execute('SELECT route_id FROM trips WHERE trip_id=?',  ( gtfs_trip_id, ))

    return gtfsdb_cursor.next()[0]


def humanize_details(route_id, details, gtfsdb_cursor, osmdb_cursor, route_db_cursor):

    def add_walk_entry(start_dist, end_dist):
        walk = 'walk (%.0f m)' % (float(start_dist) - float(end_dist))
        dist_walked = '%.2f' % float(start_dist)

        hum_details.append([ route_id, -1, walk,'', dist_walked, '', '' ])


    def add_osm_node(detail, hum_details):
        counter, label, time, dist_walked, transfers, transit_route = detail

        try:
            if float(dist_walked) > float(hum_details[-1][4]):
                add_walk_entry(dist_walked, hum_details[-1][4])

        except IndexError: # the will be an IndexError at the first entry
            pass

        label = '%s (%s)' % ( get_node_name(route_db_cursor, label), get_lat_lon(osmdb_cursor, label) )
        transit_route = ''
        dist_walked = '%.2f' % float(dist_walked)

        hum_details.append([ route_id, counter, label, time, dist_walked, transfers, transit_route ])


    hum_details = []

    if details[0][1][:4] == 'osm-':
        add_osm_node(details[0], hum_details)

    for i, ( counter, label, time, dist_walked, transfers, transit_route ) in enumerate(details):

        if label[:4] == 'osm-' or label[:4] == 'psv-': continue

        try:
            if float('%.2f'%dist_walked) > float(hum_details[-1][4]):
                add_walk_entry(dist_walked, hum_details[-1][4])

        except IndexError: # the will be an IndexError at the first entry
            pass

        # add the entry
        label = get_station_name(gtfsdb_cursor, label)
        transit_route = get_route_id(gtfsdb_cursor, transit_route) if transit_route else ''
        dist_walked = '%.2f' % float(dist_walked)

        hum_details.append([ route_id, counter, label, time, dist_walked, transfers, transit_route ])

        try: # if there is a departure at this station the information will be inside the next entry
            if details[i+1][1][:4] == 'psv-':
                transit_route = get_route_id(gtfsdb_cursor, details[i+1][5]) if details[i+1][5] else ''
                transfers = details[i+1][4]
                time = details[i+1][2]
                dist_walked = '%.2f' % float(dist_walked)

                hum_details.append([ route_id, counter, label, time, dist_walked, transfers, transit_route ])

        except IndexError: # the will be an IndexError if the last entry is a station!
            pass


    if details[-1][1][:4] == 'osm-':

        try:
            if float('%.2f'%details[-1][4]) > float(hum_details[-1][4]):
                add_walk_entry(details[-1][4], hum_details[-1][4])

        except IndexError: # the will be an IndexError at the first entry
            pass

        add_osm_node(details[-1], hum_details)

    # correct the counter
    for i, e in enumerate(hum_details):
        e[1] = i

    return hum_details


def create_indices(conn):
    c = conn.cursor()

    c.execute('CREATE INDEX IF NOT EXISTS IDX_route_id ON trips ( route_id )')
    c.execute('CREATE INDEX IF NOT EXISTS IDX_total_time ON trips ( total_time )')
    c.execute('CREATE INDEX IF NOT EXISTS IDX_trip_id ON trip_details ( trip_id )')

    conn.commit()


if __name__ == '__main__':
    import sys

    results_filename = sys.argv[1]
    details_filename = sys.argv[2]

    route_conn = sqlite3.connect(sys.argv[3], detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    route_conn.row_factory = sqlite3.Row

    osm_conn = sqlite3.connect(sys.argv[4])
    gtfs_conn = sqlite3.connect(sys.argv[5])

    osm_c = osm_conn.cursor()
    gtfs_c = gtfs_conn.cursor()

    create_indices(route_conn)

    write_results(route_conn, results_filename)
    write_details(route_conn, details_filename, gtfs_c, osm_c)

    print('done writing results')


