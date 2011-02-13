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

def write_agency(dir_name):
    global output_file_name
    if dir_name[-1] == os.sep:
        output_file_name = dir_name[:-1] + '.gtfs.zip'
    else:
        output_file_name = dir_name + '.gtfs.zip'

    agency_file = open('agency.txt', 'w')
    writer = UnicodeWriter(agency_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(( u'agency_id', u'agency_name', u'agency_url', u'agency_timezone' ))

    writer.writerow(( agency_id, dir_name, u'http://www.example.com', u'Europe/Berlin' ))

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
        values = line.split()

        id = values[0]
        lat = values[1].replace(',', '.')
        lon = values[2].replace(',', '.')
        name = ' '.join(values[3:])

        writer.writerow(( id, name, lon, lat )) # upside down, but it works that way!

    stops_file.close()

    try:
        zip_file = ZipFile(output_file_name, 'a')
        zip_file.write('stops.txt')
    finally:
        zip_file.close()

    os.remove('stops.txt')


def write_routes_tripes_stop_times(input_file_name):
    routes_file = open('routes.txt', 'w')
    trips_file = open('trips.txt', 'w')
    stop_times_file = open('stop_times.txt', 'w')

    routes_writer = UnicodeWriter(routes_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    trips_writer = writer = UnicodeWriter(trips_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    stop_times_writer = UnicodeWriter(stop_times_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    routes_writer.writerow(( u'route_id', u'agency_id', u'route_short_name', u'route_long_name', u'route_type' ))
    trips_writer.writerow(( u'route_id', u'service_id', u'trip_id' ))
    stop_times_writer.writerow(( u'trip_id', u'arrival_time', u'departure_time', u'stop_id', u'stop_sequence' ))


    f = codecs.open(input_file_name, encoding='latin-1')
    trips = []
    stop_times = []

    for line in f:

        if line[0] == '%': # comment character
            continue

        elif line[:2] == '*Z':
            trip = {}
            trips.append(trip)
            id = line.split()[1]
            stop_sequence = 1

        elif line[:2] == '*A':
            trip['service_id'] = line.split()[4]

        elif line[:2] == '*L':
            trip['route_id'] = line.split()[1]
            trip['id'] = trip['route_id'] + '-' + id + '-' + trip['service_id']

        elif line[0] == '*': # 'header' information lines start with '*'
            stop_id = line[:7]

            if line[29:33] != '    ':
                arrival_time = line[29:31] + ':' + line[31:33] + ':00'

            if line[34:38] == '    ':
                departure_time = arrival_time
            else:
                departure_time = line[34:36] + ':' + line[36:38] + ':00'

            if line[29:33] == '    ':
                arrival_time = departure_time

            stop_times.append(( trip['id'], arrival_time, departure_time, stop_id, stop_sequence ))
            stop_sequence += 1

    routes = set([ ( trip['route_id'], 1, trip['route_id'], 'unknown', 3 ) for trip in trips ])

    routes_writer.writerows( routes )
    trips_writer.writerows([ (t['route_id'], t['service_id'], t['id']) for t in trips ])
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

    for line in f:
        data = line.split()
        if data[-1] == 'Fahrplanstart': start_date = line[6:10] + line[3:5] + line[0:2]
        elif data[-1] == 'Fahrplanende': end_date = line[6:10] + line[3:5] + line[0:2]

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
        elif hex == 'A': b_list.extend(( True, False, True, False))
        elif hex == 'B': b_list.extend(( True, False, True, True))
        elif hex == 'C': b_list.extend(( True, True, False, False))
        elif hex == 'D': b_list.extend(( True, True, False, True))
        elif hex == 'E': b_list.extend(( True, True, True, False))
        elif hex == 'F': b_list.extend(( True, True, True, True))
        else: raise Exception('unrecognizable character in hex_string')

    return b_list


def increment_date_string(string):
    date = datetime.date(int(string[0:4]), int(string[4:6]), int(string[6:8]))
    date += datetime.timedelta(days=1)

    year = str(date.year)
    month = str(date.month) if len(str(date.month)) == 2 else '0' + str(date.month)
    day = str(date.day) if len(str(date.day)) == 2 else '0' + str(date.day)

    return year + month + day


def main():
    from optparse import OptionParser

    bitfield_file_name = 'bitfeld.txt'
    stops_file_name = 'bfkoord.txt'
    eckdaten_file_name = 'eckdaten.txt'
    fplan_file_name = 'FPLAN.TXT'

    usage = """usage: python hafasToGtf.py input"""
    parser = OptionParser(usage=usage)

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        exit(-1)

    if not os.path.exists(os.path.join(args[0], bitfield_file_name)) or not os.path.exists(os.path.join(args[0], eckdaten_file_name)) or not os.path.exists(os.path.join(args[0], stops_file_name)) or not os.path.exists(os.path.join(args[0], fplan_file_name)):
        print 'ERROR: invalid input data'
        parser.print_help()
        exit(-1)

    write_agency(args[0])
    write_stops(os.path.join(args[0], stops_file_name))
    write_routes_tripes_stop_times(os.path.join(args[0], fplan_file_name))
    write_calendar_calendar_dates(os.path.join(args[0], bitfield_file_name), os.path.join(args[0], eckdaten_file_name))


if __name__ == '__main__': main()
