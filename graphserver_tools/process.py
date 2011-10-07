import multiprocessing
import os
import psycopg2
import socket
import sys
import time

from termcolor import colored, cprint
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

    print('Deleting orphan nodes...')
    import_base_data.delete_orphan_nodes(db_conn_string)

    print('Linking transit to osm data...')
    import_base_data.link_osm_gtfs(db_conn_string)


    # recreate all calculation tables
    conn = psycopg2.connect(db_conn_string)

    process_routes.create_db_tables(conn, True)

    conn.commit()


def build_route_data(graph, psql_connect_string, times_filename, points_filename, routes_filename):
    conn = psycopg2.connect(psql_connect_string)

    import_route_data.read_times(times_filename, conn)
    import_route_data.read_points(points_filename, conn)
    import_route_data.read_routes(routes_filename, conn)

    # recreate all calculation tables
    process_routes.create_db_tables(conn, True)

    conn.commit()

    import_route_data.calc_corresponding_vertices(graph, psql_connect_string)


def calculate_routes(graph, psql_connect_string, options, num_processes=4):
    logfile = open('log.txt','w')
    conn = psycopg2.connect(psql_connect_string)


    process_routes.create_db_tables(conn, False)

    conn.commit()
    sys.stdout.write('created db_tables')

    prefixes = ( 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
                 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'AA', 'BB', 'CC', 'DD', 'EE',
                 'FF', 'GG', 'HH', 'II', 'JJ', 'KK', 'LL', 'MM', 'NN', 'OO', 'PP', 'QQ', 'RR',
                 'SS', 'TT', 'UU', 'VV', 'WW', 'XX', 'YY', 'ZZ' )
    processes = []

    for i in range(int(num_processes)):
        p = multiprocessing.Process(target=process_routes.Proccessing, args=(graph,
                                                                             psql_connect_string,
                                                                             int(options['time-step']),
                                                                             float(options['walking-speed']),
                                                                             int(options['max-walk']),
                                                                             int(options['walking-reluctance']),
                                                                             socket.gethostname() + prefixes[i],
                                                                             logfile))
        time.sleep(1) #workaround for duplicate calculations - should be temporary
        p.start()
        processes.append(p)
        sys.stdout.write('started thread %s \n' %i)

    status_printer = multiprocessing.Process(target=process_routes.print_status, args=(conn,logfile ))
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

    config = utils.read_config(file_path, defaults, True)

    psql_connect_string = 'dbname=%s user=%s password=%s host=%s port=%s' % ( config['psql-database'],
                                                                              config['psql-user'],
                                                                              config['psql-password'],
                                                                              config['psql-host'],
                                                                              config['psql-port']       )

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
            print('looked at: %s' % configuration['osm-data'])
            valide = False

        if not os.path.exists(configuration['transit-feed']):
            print(colored('ERROR: could not find transit-feed', 'red'))
            print('looked at: %s' % configuration['transit-feed'])
            valide = False

    if options.import_routes or options.import_all:
        if not os.path.exists(configuration['routes']):
            print(colored('ERROR: could not find routes.csv', 'red'))
            print('looked at: %s' % configuration['routes'])
            valide = False

        if not os.path.exists(configuration['times']):
            print(colored('ERROR: could not find times.csv', 'red'))
            print('looked at: %s' % configuration['times'])
            valide = False

        if not os.path.exists(configuration['points']):
            print(colored('ERROR: could not find points.csv', 'red'))
            print('looked at: %s' % configuration['points'])
            valide = False

    # check database
    base_tables = ( 'graph_vertices', 'graph_payloads', 'graph_edges', 'graph_resources',
                    'osm_nodes', 'osm_ways', 'osm_edges', 'gtfs_agency', 'gtfs_calendar',
                    'gtfs_calendar_dates', 'gtfs_frequencies', 'gtfs_routes', 'gtfs_shapes',
                    'gtfs_stop_times', 'gtfs_stops', 'gtfs_transfers', 'gtfs_trips'            )

    route_tables = ( 'cal_corres_vertices', 'cal_points', 'cal_routes', 'cal_times' )

    path_tables = ( 'cal_paths', 'cal_paths_details' )


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
                print(colored('ERROR: base data not in database - please import base data first', 'red'))

        if not options.import_routes and not options.import_all:
            error = False
            for nt in route_tables:
                if (nt,) not in tables:
                    valide = False
                    error = True
            if error:
                print(colored('ERROR: route data not in database - please import route data first', 'red'))

        if options.export and not options.calculate:
            error = False
            for nt in path_tables:
                if (nt,) not in tables:
                    valide = False
                    error = True
            if error:
                print(colored('ERROR: path data not in database - please calculate shortest paths first', 'red'))


        if options.calculate and ((not options.import_all) and (not options.import_routes)):
            c.execute('SELECT id FROM cal_routes WHERE done=false')

            if len(c.fetchall()) == 0:
                print(colored('It looks like all routes have already been calculated. Do you want to start the calculation again? [ y/n ]', 'yellow'))
                input = sys.stdin.read(1)

                if input == 'y' or input == 'Y':
                    c.execute('UPDATE cal_routes SET done=false')
                    process_routes.create_db_tables(conn, True)
                else:
                    options.calculate = False

        c.close()
        conn.commit()

    return valide


def main():
    from optparse import OptionParser

    usage = """Usage: python gst_process <configuration file>
               See the documentation for layout of the config file."""

    parser = OptionParser(usage=usage)

    parser.add_option("-b", "--import-base", action="store_true", help="imports GTFS and OSM data into the database", dest="import_base", default=False)
    parser.add_option("-r", "--import-routes", action="store_true", help="imports routing data into the database", dest="import_routes", default=False)
    parser.add_option("-i", "--import-all", action="store_true", help="imports GTFS, OSM and routing data into the database", dest="import_all", default=False)
    parser.add_option("-c", "--calculate", action="store_true", help="calculates shortest paths", dest="calculate", default=False)
    parser.add_option("-e", "--export", action="store_true", help="exports the calculted paths as CSV-files", dest="export", default=False)

    (options, args) = parser.parse_args()

    if DEBUG: print(options)

    if len(args) != 1:
        parser.print_help()
        exit(-1)

    try:
        configuration, psql_connect_string = read_config(args[0])
    except:
        print(colored('ERROR: failed reading the configuration file', 'red'))
        if DEBUG: raise
        parser.print_help()
        exit(-1)

    valide = validate_input(configuration, psql_connect_string, options)

    if not valide:
        parser.print_help()
        exit(-1)



    graph = None

    if options.import_base or options.import_all:
        print('Importing base data...')
        build_base_data(psql_connect_string, configuration['osm-data'], configuration['transit-feed'])


    if options.import_routes or options.import_all:
        print('Importing routing data...')

        graph = GraphDatabase(psql_connect_string).incarnate()

        build_route_data(graph, psql_connect_string, configuration['times'], configuration['points'], configuration['routes'])


    if options.calculate:
        print('Calculating shortest paths...')

        # only create tables if some importing was done
        create_tables = options.import_all or options.import_base or options.import_routes

        if not graph: graph = GraphDatabase(psql_connect_string).incarnate()

        start = time.time()
        calculate_routes(graph, psql_connect_string, configuration, num_processes=configuration['parallel-calculations'])
        cprint('total calculation time: %s' % utils.seconds_time_string(time.time() - start), attrs=['bold'])

    try:
        graph.destroy()
    except:
        pass


    if options.export:
        print('Exporting paths...')
        export_results(psql_connect_string, configuration['results'], configuration['result-details'])


    print('DONE')
