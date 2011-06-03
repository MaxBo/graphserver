import multiprocessing
import os
import psycopg2
import socket
import sys
import time

from termcolor import colored
from pyproj import Proj

from graphserver.graphdb import GraphDatabase

from graphserver_tools import import_base_data
from graphserver_tools import import_route_data
from graphserver_tools.utils import utils
from graphserver_tools import write_results
from graphserver_tools import process_routes


DEBUG = False



def build_base_data(db_conn_string, osm_xml_filename, gtfs_filename):


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


def read_config(file_path):

    defaults = { 'time-step':'240',
                 'max-walk':'11080',
                 'walking-reluctance':'20',
                 'walking-speed':'1.2',
                 'parallel-calculations': '4',
                 'psql-host':'localhost',
                 'psql-port':'5432',
                 'psql-user':'postgres',
                 'psql-password':'',
                 'psql-database':'graphserver',
                 'routes':'routes.csv',
                 'times':'times.csv',
                 'points':'points.csv',
                 'transit-feed':'transit_data.zip',
                 'osm-data':'streets.osm',
                 'results':'results.csv',
                 'result-details':'result-details.csv' }

    if not os.path.exists(file_path): raise Exception()

    config = utils.read_config(file_path, defaults)

    psql_connect_string = 'dbname=%s user=%s password=%s host=%s port=%s' % ( config['psql-database'],
                                                                              config['psql-user'],
                                                                              config['psql-password'],
                                                                              config['psql-host'],
                                                                              config['psql-port']       )

    if DEBUG: print(config)

    config['routes'] = os.path.join(os.path.dirname(file_path), config['routes'])
    config['times'] = os.path.join(os.path.dirname(file_path), config['times'])
    config['points'] = os.path.join(os.path.dirname(file_path), config['points'])

    config['transit-feed'] = os.path.join(os.path.dirname(file_path), config['transit-feed'])
    config['osm-data'] = os.path.join(os.path.dirname(file_path), config['osm-data'])

    config['results'] = os.path.join(os.path.dirname(file_path), config['results'])
    config['result-details'] = os.path.join(os.path.dirname(file_path), config['result-details'])


    if DEBUG: print(config)

    return config, psql_connect_string


def validate_input(configuration, psql_connect_string, options):

    valide = True

    # check input files
    if options.import_base or options.import_all:
        if not os.path.exists(configuration['osm-data']):
            print(colored('ERROR: could not find osm-data', 'red'))
            valide = False

        if not os.path.exists(configuration['transit-feed']):
            print(colored('ERROR: could not find transit-feed', 'red'))
            valide = False

    if options.import_routes or options.import_all:
        if not os.path.exists(configuration['routes']):
            print(colored('ERROR: could not find routes.csv', 'red'))
            valide = False

        if not os.path.exists(configuration['times']):
            print(colored('ERROR: could not find times.csv', 'red'))
            valide = False

        if not os.path.exists(configuration['points']):
            print(colored('ERROR: could not find points.csv', 'red'))
            valide = False

    # check database
    base_tables = ( 'graph_vertices', 'graph_payloads', 'graph_edges', 'graph_resources',
                    'osm_nodes', 'osm_ways', 'osm_edges', 'gtfs_agency', 'gtfs_calendar',
                    'gtfs_calendar_dates', 'gtfs_frequencies', 'gtfs_routes', 'gtfs_shapes',
                    'gtfs_stop_times', 'gtfs_stops', 'gtfs_transfers', 'gtfs_trips'            )

    route_tables = ( 'cal_corres_vertices', 'cal_points', 'cal_routes', 'cal_times' )

    path_tables = ( 'cal_paths', 'cal_path_details' )


    try:
        conn = psycopg2.connect(psql_connect_string)
        c = conn.cursor()

    except:
        print(colored('ERROR: could not connect to database', 'red'))

        if DEBUG: raise

        valide = False

    else:
        c.execute("select tablename from pg_tables where schemaname='public'" )
        tables = c.fetchall()

        if not options.import_base and not options.import_all:
            error = False
            for nt in base_tables:
                if (nt,) not in tables:
                    valide = False
                    error = True
            if error:
                print(colored('ERROR: base data not in database', 'red'))

        if not options.import_routes and not options.import_all:
            error = False
            for nt in route_tables:
                if (nt,) not in tables:
                    valide = False
                    error = True
            if error:
                print(colored('ERROR: route data not in database', 'red'))

        if options.export and not options.calculate:
            error = False
            for nt in path_tables:
                if (nt,) not in tables:
                    valide = False
                    error = True
            if error:
                print(colored('ERROR: path data not in database', 'red'))

    if not valide:
        exit(-1)


def main():
    from optparse import OptionParser

    usage = """usage: python gst_process <configuration file>
               See the documentation for layout of the config file."""

    parser = OptionParser(usage=usage)

    parser.add_option("-b", "--import-base", action="store_true", help="imports GTFS and OSM data into the database", dest="import_base", default=False)
    parser.add_option("-r", "--import-routes", action="store_true", help="imports routing data into the database", dest="import_routes", default=False)
    parser.add_option("-i", "--import-all", action="store_true", help="imports GTFS, OSM and routing data into the database", dest="import_all", default=False)
    parser.add_option("-c", "--calculate", action="store_true", help="calculates shortest paths", dest="calculate", default=False)
    parser.add_option("-e", "--export", action="store_true", help="exports the calculted paths as CSV-files", dest="export", default=False)

    (options, args) = parser.parse_args()


    if len(args) != 1:
        parser.print_help()
        exit(-1)

    try:
        configuration, psql_connect_string = read_config(args[0])
    except:
        print(colored('ERROR: failed reading configuration file', 'red'))
        if DEBUG: raise
        parser.print_help()
        exit(-1)

    validate_input(configuration, psql_connect_string, options)



    graph = None

    if options.import_base or options.import_all:
        print('importing base data...')
        build_base_data(psql_connect_string, configuration['osm-data'], configuration['transit-feed'])


    if options.import_routes or options.import_all:
        print('importing routes...')

        if not graph: graph = GraphDatabase(psql_connect_string).incarnate()

        build_route_data(graph, psql_connect_string, times_filename, points_filename, routes_filename)


    if options.calculate:
        print('calculation shortest paths...')

        if not graph: graph = GraphDatabase(psql_connect_string).incarnate()

        start = time.time()
        calculate_routes(graph, psql_connect_string, config, num_processes=configuration['parallel-calculations'])
        print('total calculation time: %s' % utils.seconds_time_string(time.time() - start))


    try:
        g.destroy
    except:
        pass


    if options.export:
        print('writing results...')
        export_results(psql_connect_string, results_filename, result_details_filename)


    print('DONE')
