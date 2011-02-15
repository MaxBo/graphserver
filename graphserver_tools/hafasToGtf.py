# -*- coding: utf-8 -*-

#
# Author:   Tobias Ottenweller
# Date:     10.02.2011
#
# Gertz Gutsche Rümenapp Gbr
#

from zipfile import ZipFile
import datetime, codecs, csv, os

from graphserver_tools.utf8csv import UnicodeWriter

agency_id = 1
route_type = 3
output_file_name = None


''' this function writes a dummy agency_file file so that a valid gtfs-feed will be created

'''
def write_agency():
    agency_file = open('agency.txt', 'w')
    writer = UnicodeWriter(agency_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(( u'agency_id', u'agency_name', u'agency_url', u'agency_timezone' ))

    writer.writerow(( agency_id, u'SomeAgency', u'http://www.example.com', u'Europe/Berlin' ))

    agency_file.close()

    try:
        zip_file = ZipFile(output_file_name, 'a')
        zip_file.write('agency.txt')
    finally:
        zip_file.close()

    os.remove('agency.txt')


def write_stops(input_file_name):
    stops_file = open('stops.txt', 'w')
    writer = UnicodeWriter(stops_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(( u'stop_id', u'stop_name', u'stop_lat', u'stop_lon' ))

    f = codecs.open(input_file_name, encoding='latin-1')

    for line in f:
        if line[0] == '%':
            continue

        values = line.split()

        id = values[0]
        lon = values[1].replace(',', '.')
        lat = values[2].replace(',', '.')
        name = ' '.join(values[3:])

        writer.writerow(( id, name, lat, lon ))

    stops_file.close()

    try:
        zip_file = ZipFile(output_file_name, 'a')
        zip_file.write('stops.txt')
    finally:
        zip_file.close()

    os.remove('stops.txt')


def get_routes_tripes_stop_times(input_file_name):

    f = codecs.open(input_file_name, encoding='latin-1')
    trips = []
    stop_times = []

    for line in f:

        if line[0] == '%': # comment character
            continue

        elif line[:2] == '*Z': # new 'trip'
            trip = {}
            trips.append(trip)
            id = line.split()[1]
            stop_sequence = 1

        elif line[:5] == '*A VE':
            trip['service_id'] = line.split()[4]

        elif line[:2] == '*L':
            trip['route_id'] = line.split()[1]

        elif line[:2] == '*R':
            trip['direction'] = line.split()[1]

        elif not line[0] == '*': # 'header' information lines start with '*'
            stop_id = line[:7]

            if line[29:33] != '    ' and line[29:33] != '9999':
                arrival_time = line[29:31] + ':' + line[31:33] + ':00'

            if line[34:38] == '    ' or line[34:38] == '9999':
                departure_time = arrival_time
            else:
                departure_time = line[34:36] + ':' + line[36:38] + ':00'

            if line[29:33] == '    ' or line[29:33] == '9999':
                arrival_time = departure_time

            if 'trip_id' not in trip:
                trip['trip_id'] = id + '-' + trip['route_id'] + '-' + trip['direction'] + '-' + trip['service_id'] + '-' + arrival_time

            stop_times.append(( trip['trip_id'], arrival_time, departure_time, stop_id, stop_sequence ))
            stop_sequence += 1

    routes = set([ ( trip['route_id'], 1, trip['route_id'], 'unknown', 3 ) for trip in trips ])
    trips = [ (t['route_id'], t['service_id'], t['trip_id']) for t in trips ]

    return routes, trips, stop_times


def write_routes_tripes_stop_times(input_file_names):

    routes = []
    trips = []
    stop_times = []

    for f in input_file_names:
        r, t, st = get_routes_tripes_stop_times(f)
        routes.extend(r)
        trips.extend(t)
        stop_times.extend(st)

    routes = set(routes)

    routes_file = open('routes.txt', 'w')
    trips_file = open('trips.txt', 'w')
    stop_times_file = open('stop_times.txt', 'w')

    routes_writer = UnicodeWriter(routes_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    trips_writer = writer = UnicodeWriter(trips_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    stop_times_writer = UnicodeWriter(stop_times_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    routes_writer.writerow(( u'route_id', u'agency_id', u'route_short_name', u'route_long_name', u'route_type' ))
    trips_writer.writerow(( u'route_id', u'service_id', u'trip_id' ))
    stop_times_writer.writerow(( u'trip_id', u'arrival_time', u'departure_time', u'stop_id', u'stop_sequence' ))

    routes_writer.writerows( routes )
    trips_writer.writerows( trips )
    stop_times_writer.writerows( stop_times )

    routes_file.close()
    trips_file.close()
    stop_times_file.close()

    try:
        zip_file = ZipFile(output_file_name, 'a')
        zip_file.write('routes.txt')
        zip_file.write('trips.txt')
        zip_file.write('stop_times.txt')
    finally:
        zip_file.close()

    os.remove('routes.txt')
    os.remove('trips.txt')
    os.remove('stop_times.txt')


def write_calendar_calendar_dates(bitfield_file_name, eckdaten_file_name):
    calendar_file = open('calendar.txt', 'w')
    calendar_dates_file = open('calendar_dates.txt', 'w')

    f = codecs.open(eckdaten_file_name, encoding='latin-1')

    lines = [ l for l in f if l[0] != '%' ]
    start_date = lines[0][6:10] + lines[0][3:5] + lines[0][0:2]
    end_date = lines[1][6:10] + lines[1][3:5] + lines[1][0:2]

    c_writer = UnicodeWriter(calendar_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    c_writer.writerow(( u'service_id', u'monday', u'tuesday', u'wednesday', u'thursday', u'friday', u'saturday', u'sunday', u'start_date', u'end_date' ))
    cd_writer = UnicodeWriter(calendar_dates_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    cd_writer.writerow(( u'service_id', u'date', u'exception_type' ))

    f = codecs.open(bitfield_file_name, encoding='latin-1')

    for line in f:
        date = start_date
        id, hex_field = line.split()
        c_writer.writerow(( id, 1, 1, 1, 1, 1, 1, 1, start_date, end_date ))

        for bool in hex_to_bool_list(hex_field):
            if not bool:
                cd_writer.writerow(( id, date, 2 ))
            date = increment_date_string(date)

    calendar_file.close()
    calendar_dates_file.close()

    try:
        zip_file = ZipFile(output_file_name, 'a')
        zip_file.write('calendar.txt')
        zip_file.write('calendar_dates.txt')
    finally:
        zip_file.close()

    os.remove('calendar.txt')
    os.remove('calendar_dates.txt')


def hex_to_bool_list(hex_string):
    b_list = []

    for hex in hex_string:
        if hex == '0': b_list.extend(( False, False, False, False))
        elif hex == '1': b_list.extend(( False, False, False, True))
        elif hex == '2': b_list.extend(( False, False, True, False))
        elif hex == '3': b_list.extend(( False, False, True, True))
        elif hex == '4': b_list.extend(( False, True, False, False))
        elif hex == '5': b_list.extend(( False, True, False, True))
        elif hex == '6': b_list.extend(( False, True, True, False))
        elif hex == '7': b_list.extend(( False, True, True, True))
        elif hex == '8': b_list.extend(( True, False, False, False))
        elif hex == '9': b_list.extend(( True, False, False, True))
        elif hex == 'A' or hex == 'a': b_list.extend(( True, False, True, False))
        elif hex == 'B' or hex == 'b': b_list.extend(( True, False, True, True))
        elif hex == 'C' or hex == 'c': b_list.extend(( True, True, False, False))
        elif hex == 'D' or hex == 'd': b_list.extend(( True, True, False, True))
        elif hex == 'E' or hex == 'e': b_list.extend(( True, True, True, False))
        elif hex == 'F' or hex == 'f': b_list.extend(( True, True, True, True))
        else: raise Exception('unrecognizable character in hex_string')

    return b_list


def increment_date_string(string):
    date = datetime.date(int(string[0:4]), int(string[4:6]), int(string[6:8]))
    date += datetime.timedelta(days=1)

    year = str(date.year)
    month = str(date.month) if len(str(date.month)) == 2 else '0' + str(date.month)
    day = str(date.day) if len(str(date.day)) == 2 else '0' + str(date.day)

    return year + month + day


def find_files(input_dir):
    found_dic = {}

    for f in os.listdir(input_dir):
        ff = f.lower().split('.')[0]

        if ff == 'bitfeld':
            found_dic['bitfeld'] = f

        elif ff == 'fplan':
            if 'fplan' in found_dic:
                return {}
            else:
                found_dic['fplan'] = ( f, )

        elif ff == 'eckdaten':
            found_dic['eckdaten'] = f

        elif ff == 'alldat':
            if 'fplan' in found_dic:
                return {}
            else:
                found_dic['fplan'] = [ l[:-2] for l in open(os.path.join(input_dir, f)) ]

        elif ff == 'bfkoord':
            found_dic['bfkoord'] = f

    return found_dic


def main():
    from optparse import OptionParser

    usage = """usage: python hafasToGtf.py input"""
    parser = OptionParser(usage=usage)

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        exit(-1)

    input_dir = args[0]

    # set the output file name
    global output_file_name
    if input_dir[-1] == os.sep:
        output_file_name = input_dir[:-1] + '.gtfs.zip'
    else:
        output_file_name = input_dir + '.gtfs.zip'

    if not os.path.exists(input_dir):
        print 'ERROR: invalid input'
        parser.print_help()
        exit(-1)

    if os.path.exists(output_file_name):
        print 'ERROR: output file already exists!'
        parser.print_help()
        exit(-1)

    files = find_files(input_dir)

    # validate the input
    if 'bitfeld' not in files:
        print 'ERROR: invalid input data (bitfeld)'
        parser.print_help()
        exit(-1)
    if 'fplan' not in files:
        print 'ERROR: invalid input data (fplan)'
        parser.print_help()
        exit(-1)
    if 'eckdaten' not in files:
        print 'ERROR: invalid input data (eckdaten)'
        parser.print_help()
        exit(-1)
    if 'bfkoord' not in files:
        print 'ERROR: invalid input data (bfkoord)'
        parser.print_help()
        exit(-1)

    # do the convertion
    write_agency()
    write_stops(os.path.join(input_dir, files['bfkoord']))
    write_calendar_calendar_dates(os.path.join(input_dir, files['bitfeld']), os.path.join(input_dir, files['eckdaten']))

    write_routes_tripes_stop_times( [ os.path.join(input_dir, f) for f in files['fplan'] ] )


if __name__ == '__main__': main()
