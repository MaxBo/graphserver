import os
from pyproj import Proj
from graphserver.graphdb import GraphDatabase
import sys
import sqlite3
import psycopg2
import multiprocessing
import time
import socket

from graphserver_tools import import_base_data
from graphserver_tools import import_route_data
from graphserver_tools.utils import utils
from graphserver_tools import write_results
from graphserver_tools import process_routes


def build_base_data(dir_name, db_conn_string):
    gtfs_filename = os.path.join(dir_name, 'transit_data.zip')
    osm_xml_filename = os.path.join(dir_name, 'streets.osm')

    if not os.path.exists(osm_xml_filename):
        print('ERROR: no osm data found!')
        exit(-1)

    if not os.path.exists(gtfs_filename):
        print('ERROR: no transit data found!')
        exit(-1)

    print('importing data into (graph) databases...')
    import_base_data.create_gs_datbases(osm_xml_filename, gtfs_filename, db_conn_string)

    import_base_data.add_missing_stops(db_conn_string)

    print('deleting orphan nodes...')
    import_base_data.delete_orphan_nodes(db_conn_string)

    print('linking transit to osm data...')
    import_base_data.link_osm_gtfs(db_conn_string)


def build_route_data(graph, psql_connect_string, times_filename, points_filename, routes_filename):
    conn = psycopg2.connect(psql_connect_string)

    import_route_data.read_times(times_filename, conn)
    import_route_data.read_points(points_filename, conn)
    import_route_data.read_routes(routes_filename, conn)

    conn.commit()

    import_route_data.calc_corresponding_vertices(graph, psql_connect_string)


def calculate_routes(graph, psql_connect_string, options, num_processes=4):
    conn = psycopg2.connect(psql_connect_string)
    process_routes.create_db_tables(conn)
    conn.commit()

    prefixes = ( 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P' )
    processes = []

    for i in range(4):
        p = multiprocessing.Process(target=process_routes.Proccessing, args=(graph,
                                                                             psql_connect_string,
                                                                             int(options['time-step']),
                                                                             float(options['walking-speed']),
                                                                             int(options['max-walk']),
                                                                             int(options['walking-reluctance']),
                                                                             socket.gethostname() + prefixes[i]))
        time.sleep(1) #workaround for duplicate calculations - should be temporary
        p.start()
        processes.append(p)

    status_printer = multiprocessing.Process(target=process_routes.print_status, args=(conn, ))
    status_printer.start()
    processes.append(status_printer)

    for p in processes:
        p.join()


def export_results(psql_connect_string, results_filename, result_details_filename):
    conn = psycopg2.connect(psql_connect_string)

    write_results.create_indices(conn)

    write_results.write_results(conn, results_filename)
    write_results.write_details(conn, result_details_filename)


def main():
    from optparse import OptionParser

    usage = """usage: python gst_process calculation-folder.
               Note: a special file hirarchie is neccessary in this folder. See documentaion for further information."""
    parser = OptionParser(usage=usage)
    parser.add_option("-f", "--forceinit", action="store_true", help="forces to reload all data", dest="overwrite", default=False)

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        exit(-1)

    dir_name = args[0]
    overwrite = options.overwrite

    times_filename = os.path.join(dir_name, 'times.csv')
    points_filename = os.path.join(dir_name, 'points.csv')
    routes_filename = os.path.join(dir_name, 'routes.csv')
    results_filename = os.path.join(dir_name, 'results.csv')
    result_details_filename = os.path.join(dir_name, 'result-details.csv')

    if not os.path.exists(times_filename) or not os.path.exists(points_filename) or not os.path.exists(routes_filename):
        print('ERROR: could not find one or more input files')
        parser.print_help()
        exit(-1)

    # read the configuration
    defaults = { 'time-step':'240',
                 'max-walk':'11080',
                 'walking-reluctance':'20',
                 'walking-speed':'1.2',
                 'psql-host':'localhost',
                 'psql-port':'5432',
                 'psql-user':'postgres',
                 'psql-password':'',
                 'psql-database':'graphserver' }

    config = utils.read_config(os.path.join(dir_name, 'config.txt'), defaults)
    psql_connect_string = 'dbname=%s user=%s password=%s host=%s port=%s' % ( config['psql-database'],
                                                                              config['psql-user'],
                                                                              config['psql-password'],
                                                                              config['psql-host'],
                                                                              config['psql-port'] )

    conn = psycopg2.connect(psql_connect_string)
    c = conn.cursor()

    c.execute("select tablename from pg_tables where schemaname='public'" )
    tables = c.fetchall()

    needed_tables = ( 'graph_vertices', 'graph_payloads', 'graph_edges', 'graph_resources',
                      'osm_nodes', 'osm_ways', 'osm_edges', 'gtfs_agency', 'gtfs_calendar',
                      'gtfs_calendar_dates', 'gtfs_frequencies', 'gtfs_routes', 'gtfs_shapes',
                      'gtfs_stop_times', 'gtfs_stops', 'gtfs_transfers', 'gtfs_trips' )

    for nt in needed_tables:
        if (nt,) not in tables:
            overwrite = True

    c.close()
    conn.close()

    if overwrite:
        build_base_data(dir_name, psql_connect_string)

    g = GraphDatabase(psql_connect_string).incarnate()

    print('importing routing data...')
    build_route_data(g, psql_connect_string, times_filename, points_filename, routes_filename)


    print('calculation shortest paths...')
    start = time.time()
    calculate_routes(g, psql_connect_string, config, num_processes=4)

    print('total calculation time: %s' % utils.seconds_time_string(time.time() - start))
    g.destroy

    print('writing results...')
    export_results(psql_connect_string, results_filename, result_details_filename)

    print('DONE')
