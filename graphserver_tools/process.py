import os
from pyproj import Proj
from graphserver.graphdb import GraphDatabase
import sys
import sqlite3

from graphserver_tools import import_base_data
from graphserver_tools import import_route_data
from graphserver_tools.utils import utils
from graphserver_tools import write_results
from graphserver_tools import process_routes



def build_base_data():
    gtfs_filename = os.path.join('01-Basisdaten', 'transit_data.zip')
    osm_xml_filename = os.path.join('01-Basisdaten', 'streets.osm')
    osmdb_filename = os.path.join('XX-System', 'streets.osmdb')
    gtfsdb_filename = os.path.join('XX-System', 'transit_feed.gtfsdb')
    gsdb_filename = os.path.join('XX-System', 'graph.db')

    try:
        os.mkdir('XX-System')
    except:
        if os.path.exists('XX-System'):
            print('WARNING: XX-System already exists! - may lead to errors')
        else:
            print('ERROR: cannot create folder XX-System')
            exit(-1)

    if not os.path.exists(osm_xml_filename):
        print('ERROR: no osm data found!')
        exit(-1)

    if not os.path.exists(gtfs_filename):
        print('ERROR: no transit data found!...')
        exit(-1)

    print('importing data into (graph) databases...')
    import_base_data.create_gs_datbases(osm_xml_filename, osmdb_filename, gtfs_filename, gtfsdb_filename, gsdb_filename)
    import_base_data.add_missing_stops(gtfsdb_filename, gsdb_filename)
    import_base_data.delete_orphan_nodes(osmdb_filename)

    print('linking transit to osm data...')
    graph_database = GraphDatabase(gsdb_filename)
    import_base_data.link_osm_gtfs(gtfsdb_filename, osmdb_filename, gsdb_filename)


def main():
    from optparse import OptionParser

    usage = """usage: python gst_process calculation-folder.\nNote: a special file hirarchie is neccessary in this folder. See documentaion for further information."""
    parser = OptionParser(usage=usage)

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        exit(-1)

    dir_name = args[0]

    times_filename = os.path.join(dir_name, 'times.csv')
    points_filename = os.path.join(dir_name, 'points.csv')
    routes_filename = os.path.join(dir_name, 'routes.csv')
    graphdb_filename = os.path.join('XX-System', 'graph.db')
    gtfsdb_filename = os.path.join('XX-System', 'transit_feed.gtfsdb')
    osmdb_filename = os.path.join('XX-System', 'streets.osmdb')
    results_filename = os.path.join(dir_name, 'results.csv')
    result_details_filename = os.path.join(dir_name, 'result_details.csv')
    routingdb_filename = os.path.join(dir_name, 'routing.db')

    if not os.path.exists(times_filename) or not os.path.exists(points_filename) or not os.path.exists(routes_filename):
        print('ERROR: could not find one or more input files')
        parser.print_help()
        exit(-1)


    if not os.path.exists(os.path.join('XX-System', 'graph.db')):
        build_base_data()

    try:
        os.remove(routingdb_filename)
    except:
        if os.path.exists(routingdb_filename):
        	print('ERROR: could not remove old routing database')
        	parser.print_help()
        	exit(-1)

    g = GraphDatabase(graphdb_filename).incarnate()

    print('importing routing data...')
    conn = sqlite3.connect(routingdb_filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row

    import_route_data.read_times(times_filename, conn)
    import_route_data.read_points(points_filename, conn)
    import_route_data.read_routes(routes_filename, conn)

    import_route_data.calc_corresponding_vertices(conn, g ,osmdb_filename, gtfsdb_filename)

    conn.commit()

    print('calculation shortest paths...')
    defaults = { 'time-step':'240', 'max-walk':'11080', 'walking-reluctance':'20', 'walking-speed':'1.2' }
    config = utils.read_config(os.path.join(dir_name, 'config.txt'), defaults)

    process_routes.Proccessing(g, routingdb_filename, int(config['time-step']), float(config['walking-speed']), int(config['max-walk']), int(config['walking-reluctance']))

    g.destroy()

    print('writing results...')
    route_conn = sqlite3.connect(routingdb_filename, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    route_conn.row_factory = sqlite3.Row

    osm_conn = sqlite3.connect(osmdb_filename)
    gtfs_conn = sqlite3.connect(gtfsdb_filename)

    write_results.create_indices(route_conn)

    write_results.write_results(route_conn, results_filename)
    write_results.write_details(route_conn, result_details_filename, gtfs_conn, osm_conn)

    print('DONE')