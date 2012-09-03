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
    """Import OSM and GTFS data into database and link them"""
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
    """Import route data (routes, timetables and the points to be calculated) from csv files into database"""
    conn = psycopg2.connect(psql_connect_string)

    import_route_data.read_times(times_filename, conn)
    import_route_data.read_points_0(points_filename, conn)
    import_route_data.read_routes_0(routes_filename, conn)

    # recreate all calculation tables
    process_routes.create_db_tables(conn, True)

    conn.commit()

    import_route_data.calc_corresponding_vertices(graph, psql_connect_string)


def calculate_routes(graph, psql_connect_string, options, num_processes=4, write_cal_paths_details=False, fast_calc=False):
    """Calculate the shortest paths

    Keyword arguments:
    graph -- the graph
    psql_connect_string -- database connection
    options -- passed arguments of the main
    num_processes -- number of parallel calculations

    """
    logfile = open('log.txt','w')
    conn = psycopg2.connect(psql_connect_string)


    process_routes.create_db_tables(conn, False)

    conn.commit()
    sys.stdout.write('created db_tables\n')

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
                                                                             float(options['walking-reluctance']),
                                                                             socket.gethostname() + prefixes[i],
                                                                             logfile,
                                                                             write_cal_paths_details,
                                                                             int(options['max-travel-time']),
                                                                             fast_calc))
        p.start()
        sys.stdout.write('started thread %s \n' %i)
        time.sleep(10) #workaround for duplicate calculations - should be temporary
        processes.append(p)

    status_printer = multiprocessing.Process(target=process_routes.print_status, args=(conn,logfile ))
    status_printer.start()
    processes.append(status_printer)

    for p in processes:
        p.join()


def export_results(psql_connect_string, results_filename, result_details_filename):
    """Write the results of the calculation from database into files"""
    conn = psycopg2.connect(psql_connect_string)

    write_results.create_indices(conn)

    write_results.write_results(conn, results_filename)
    write_results.write_details(conn, result_details_filename)


def read_config(file_path):
    """Read the configuration file at the passed file path"""

    defaults = { 'time-step':'240',
                 'max-walk':'11080',
                 'walking-reluctance':'20',
                 'walking-speed':'1.2',
                 'parallel-calculations': '4',
                 'max-travel-time': '25200',
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
    """Validation of the configuration and the passed options

    Check if the input data is existing at the specified paths
    Check the Database

    Return true if valid

    """
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

        if options.export and not options.calculate:
            for nt in path_tables:
                if (nt,) not in tables:
                    valide = False
                    print(colored('ERROR: path data not in database - please calculate shortest paths first', 'red'))
                    break
            if not validate_tables(psql_connect_string, ('cal_paths', 'cal_path_details')): valide = False   

        if options.import_routes:
            if not validate_tables(psql_connect_string, ('destinations','origins','cal_times','gtfs_stop_times')): 
                valide, options.calculate = False, False
            

        if options.calculate and ((not options.import_all) and (not options.import_routes)):            
            for nt in route_tables:
                if (nt,) not in tables:
                    options.calculate, valide = False, False
                    print(colored('ERROR: route data not in database - please import route data first', 'red'))
                    break
            if not validate_tables(psql_connect_string, ('gtfs_stop_times','cal_routes')):
                options.calculate, valide = False, False
        
        if options.calculate:
            if not validate_tables(psql_connect_string, ('cal_times',)): valide = False
            else:
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
        conn.close()

    return valide

def validate_tables(psql_connect_string, tables):
    """validate the given tables (tuples or lists needed!) for correctness"""
    try:
        conn = psycopg2.connect(psql_connect_string)
        c = conn.cursor()
        c2 = conn.cursor()

    except:
        print(colored('ERROR: could not connect to database', 'red'))
        return False
    if 'destinations' in tables or 'origin' in tables:
        min_lat, max_lat, min_lon, max_lon = get_osm_borders(psql_connect_string)
    valid = True
    for table in tables:
        
        if table == 'gtfs_stop_times':  #check for time travels (arrival time before departure time)
            c.execute('''SELECT g2.trip_id, g2.arrival_time, g1.departure_time, g1.stop_sequence, g2.stop_sequence
                        FROM gtfs_stop_times g1 INNER JOIN gtfs_stop_times g2 ON g1.trip_id = g2.trip_id
                        WHERE g1.trip_id = g2.trip_id
                        AND g1.stop_sequence < g2.stop_sequence
                        AND g2.arrival_time < g1.departure_time
                        ORDER BY trip_id, g1.stop_sequence''')
            row = c.fetchone()
            if row:
                valid = False
                print(colored('Time Travel detected in table gtfs_stop_times @ trip_id %s: arrival time earlier than preceding departure time' %row[0], 'red'))
                print('departure time at stop sequence %i: %i'%(row[3],row[2]))
                print('arrival time at stop sequence %i: %i'%(row[4], row[1]))
                print
                break
        
        if table == 'cal_routes':
            c.execute('SELECT COUNT(id) FROM cal_routes')
            num_routes = c.fetchone()
            num_origs = c.execute('SELECT COUNT(name) FROM origins')
            num_origs = c.fetchone()
            num_dests = c.execute('SELECT COUNT(name) FROM destinations')
            num_dests = c.fetchone()
            if num_routes[0] != num_origs[0] * num_dests[0]:
                print(colored('The numbers of origins and destinations don\'t match the number of routes, maybe you should import the route data again. Do you want to start the calculation anyway? [ y/n ]', 'yellow'))
                input = sys.stdin.read(1)

                if input == 'y' or input == 'Y':
                    c.execute('UPDATE cal_routes SET done=false')
                    process_routes.create_db_tables(conn, True)
                else:
                    valid = False
                 
               
        c.execute('SELECT * FROM %s' %table)
        row = c.fetchone()
        if not row:
            print(colored('Table %s is empty' %table, 'red'))
            valid = False
            
        while row:
            if table == 'gtfs_stop_times':
                if row[1] > row[2]:
                    valid = False
                    print(colored('Error in table gtfs_stop_times @ id %s: arrival time is later than departure time in same row' %row[0], 'red'))
                    print row
                    print
                   
            if table=='cal_times':
                id, start_time, end_time, is_arrival = row
                if start_time > end_time: 
                    valid = False
                    print(colored('Error in table cal_times @ id %i: start time is later than end time' %id, 'red'))
                    print ('%d, %s, %s, %s' %(id, str(start_time), str(end_time), is_arrival))
                    print
                for i in (start_time, end_time):
                    try:
                        valid_date = time.strptime(str(i), '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        valid = False
                        print(colored('Error in table cal_times @ id %i: invalid date format: %s' %(id, str(i)), 'red'))
                        print ('%d, %s, %s, %s' %(id, str(start_time), str(end_time), is_arrival))
                        print
            
            if table=='destinations' or table=='origins':
                if table=='destinations': 
                    name, lat, lon, time_id = row
                    c2.execute('SELECT * FROM cal_times WHERE id = %i' %time_id) #check if there are times in cal_times that match the time_id of the destination
                    if not c2.fetchone():
                        valid = False
                        print(colored('Error in table destinations @ name %s: time id \'%i\' is not found in cal_times' %(name, time_id), 'red'))
                        print row
                        print
                else: name, lat, lon = row
                if not (max_lat >= lat >= min_lat and max_lon >= lon >= min_lon):
                    valid = False
                    print(colored('Error in table %s @ name %s: lat/lon not within OSM Area (min_lat: %f; max_lat: %f;  min_lon %f; max_lon: %f' %(table, name, min_lat, max_lat, min_lon, max_lon), 'red'))
                    print row
                    print
            
            row = c.fetchone()
            
    c.close()
    c2.close()
    conn.close()
    return valid

def get_osm_borders(psql_connect_string):
    try:
        conn = psycopg2.connect(psql_connect_string)
        c = conn.cursor()
    except: pass
    c.execute('SELECT MIN(lat) FROM osm_nodes')
    min_lat = c.fetchone()[0]
    c.execute('SELECT MAX(lat) FROM osm_nodes')
    max_lat = c.fetchone()[0]
    c.execute('SELECT MIN(lon) FROM osm_nodes')
    min_lon = c.fetchone()[0]
    c.execute('SELECT MAX(lon) FROM osm_nodes')
    max_lon = c.fetchone()[0] 
    c.close()
    conn.close()
    return min_lat, max_lat, min_lon, max_lon
    

def main():
    from optparse import OptionParser

    usage = """Usage: python process <configuration file>
               See the documentation for layout of the config file."""

    parser = OptionParser(usage=usage)

    parser.add_option("-b", "--import-base", action="store_true", help="imports GTFS and OSM data into the database", dest="import_base", default=False)
    parser.add_option("-r", "--import-routes", action="store_true", help="imports routing data into the database", dest="import_routes", default=False)
    parser.add_option("-i", "--import-all", action="store_true", help="imports GTFS, OSM and routing data into the database", dest="import_all", default=False)
    parser.add_option("-c", "--calculate", action="store_true", help="calculates shortest paths", dest="calculate", default=False)
    parser.add_option("-d", "--details", action="store_true", help="exports the calculted paths-details into the database", dest="details", default=False)
    parser.add_option("-e", "--export", action="store_true", help="exports the calculted paths as CSV-files", dest="export", default=False)
    parser.add_option("-f", "--fast-calc", action="store_true", help="experimental faster calculation", dest="fast_calc", default=False)

    (options, args) = parser.parse_args()

    #starttime = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime())

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
        #       UNUSED!
        #create_tables = options.import_all or options.import_base or options.import_routes
        if not graph: graph = GraphDatabase(psql_connect_string).incarnate()

        start = time.time()
        calculate_routes(graph, psql_connect_string, configuration, num_processes=configuration['parallel-calculations'], write_cal_paths_details=options.details, fast_calc=options.fast_calc)
        cprint('total calculation time: %s' % utils.seconds_time_string(time.time() - start), attrs=['bold'])

    try:
        graph.destroy()
    except:
        pass


    if options.export:
        print('Exporting paths...')
        export_results(psql_connect_string, configuration['results'], configuration['result-details'])

    #print ("Startzeitpunkt:" + starttime)
    #print ("Endzeitpunkt:" + time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime()))
    print('DONE')