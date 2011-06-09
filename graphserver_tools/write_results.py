#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 29.10.2010
# Gertz Gutsche Rümenapp Gbr

from graphserver_tools.utils import utf8csv


def write_details(conn, filename):
    writer = utf8csv.UnicodeWriter(open(filename, 'w'))

    writer.writerow(( u'route_id', u'counter', u'label', u'arrival/departure', u'dist_walked', u'transfers', u'transit_route' ))

    c = conn.cursor()

    c.execute('SELECT route_id FROM cal_paths')
    routes = set(c.fetchall())


    for r, in sorted(routes, key=lambda route: route[0]):
        c.execute('''SELECT id, route_id
                     FROM cal_paths
                     WHERE total_time=( SELECT MIN(total_time) FROM cal_paths WHERE route_id=%s )
                     AND route_id=%s''', ( r, r ))
        id, route_id = c.fetchone()

        c.execute('''SELECT counter, label, time, dist_walked, num_transfers, gtfs_trip_id
                     FROM cal_paths_details
                     WHERE path_id=%s''', ( id, ))
        details = c.fetchall()

        if details: # there will be no details if there is no path between origin and destination at this trip
            writer.writerows(humanize_details(route_id, details, conn))

    conn.commit()
    c.close()


def write_results(conn, filename):
    writer = utf8csv.UnicodeWriter(open(filename, 'w'))

    writer.writerow(( u'route_id', u'start_time', u'end_time', u'total_time' ))

    c = conn.cursor()

    c.execute('SELECT route_id FROM cal_paths')
    routes = set(c.fetchall())


    for r, in sorted(routes, key=lambda route: route[0]):
        c.execute('''SELECT route_id, start_time, end_time, total_time
                     FROM cal_paths
                     WHERE total_time=( SELECT MIN(total_time) FROM cal_paths WHERE route_id=%s )
                     AND route_id=%s''', ( r, r ))
        fastest_trip = list(c.fetchone())

        fastest_trip[3] = '%i:%02i:%02i' % ( fastest_trip[3]/3600, (fastest_trip[3]/60)%60, fastest_trip[3]%60 )

        writer.writerow(fastest_trip)

    c.close()


def get_lat_lon(conn, gs_osm_vertex):
    cursor = conn.cursor()

    cursor.execute('SELECT lat, lon FROM osm_nodes WHERE id=%s', ( gs_osm_vertex[4:], ))

    try: # bad hack for weired error - hope it works
        lat, lon = cursor.fetchone()
    except:
        print("Error fetching lat,lon from database ( ID=%s ). Retrying ..." % gs_osm_vertex)
        return get_lat_lon(conn, gs_osm_vertex)

    cursor.close()
    return '%.4f, %.4f' % ( lat, lon )


def get_node_name(conn, node_label):
    cursor = conn.cursor()
    cursor.execute('SELECT point_id FROM cal_corres_vertices WHERE vertex_label=%s', ( node_label, ))
    try:
        pid, = route_db_cursor.fetchone()
    except:
        return ''

    cursor.execute('SELECT name FROM cal_points WHERE id=%s', ( pid, ))
    ret = cursor.fetchone()[0]

    cursor.close()
    return ret


def get_station_name(conn, gs_sta_vertex):
    cursor = conn.cursor()

    cursor.execute('SELECT stop_name FROM gtfs_stops WHERE stop_id=%s', ( gs_sta_vertex[4:], ))
    ret = cursor.next()[0]

    cursor.close()
    return ret


def get_route_id(conn, gtfs_trip_id):
    cursor = conn.cursor()

    cursor.execute('SELECT route_id FROM gtfs_trips WHERE trip_id=%s',  ( gtfs_trip_id, ))
    ret = cursor.next()[0]

    cursor.close()
    return ret


def humanize_details(route_id, details, conn):

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

        label = '%s (%s)' % ( get_node_name(conn, label), get_lat_lon(conn, label) )
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
        label = get_station_name(conn, label)
        transit_route = get_route_id(conn, transit_route) if transit_route else ''
        dist_walked = '%.2f' % float(dist_walked)

        hum_details.append([ route_id, counter, label, time, dist_walked, transfers, transit_route ])

        try: # if there is a departure at this station the information will be inside the next entry
            if details[i+1][1][:4] == 'psv-':
                transit_route = get_route_id(conn, details[i+1][5]) if details[i+1][5] else ''
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

    c.execute('DROP INDEX IF EXISTS IDX_route_id')
    c.execute('CREATE INDEX IDX_route_id ON cal_paths ( route_id )')
    c.execute('DROP INDEX IF EXISTS IDX_total_time')
    c.execute('CREATE INDEX IDX_total_time ON cal_paths ( total_time )')
    c.execute('DROP INDEX IF EXISTS IDX_path_id')
    c.execute('CREATE INDEX IDX_path_id ON cal_paths_details ( path_id )')

    c.close()
    conn.commit()
