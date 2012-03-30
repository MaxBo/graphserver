#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Author:   Tobias Ottenweller
# Date:     11.8.2010 - 12.11.2010
#
# Gertz Gutsche Rümenapp Gbr
#

import re
from zipfile import ZipFile
import os
import sys
import codecs
import csv
from pyproj import Proj, transform

from graphserver_tools.utils.utf8csv import UnicodeWriter
from graphserver_tools.utils.utils import read_config, time_adder, coord_to_wgs84, eliminate_blank_lines


# TODO: - error handling
#       - documenting
#       - read config file


class InvalidInputException(Exception):
    pass


class NetToGtf():

    def _write_calendar(self, table_header_line): # reads from $VERKEHRSTAG and ???
        # TODO: at this moment I'm not sure how to deal with calendar.txt
        #       so there will be just a single daily entry!

        calendar_header = ( u'service_id', u'monday', u'tuesday', u'wednesday', u'thursday',
                            u'friday', u'saturday', u'sunday', u'start_date', u'end_date' )

        calendar_daily_entry = ( '1', '1', '1', '1', '1', '1', '1', '1', '20000101', '20201231' )

        f = open('calendar.txt', 'w')
        writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        writer.writerow(calendar_header)
        writer.writerow(calendar_daily_entry)
        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('calendar.txt')
        finally:
            zip_file.close()

        os.remove('calendar.txt')


    def _process_route_types(self, table_header_line): # reads from $VSYS
        ''' Creates a dictionary [route_types_map] to map between .net file's $VSYS CODE
            and in gtfs specified route-types codes. It uses information read in from the
            config file [net_route_types_mapper]. If none information given a default value
            (3/Bus) will be set.
        '''
        if self.debug: print u'processing route_types'

        self.route_types_map = {}
        columns = table_header_line.split(':')[1].split(';')

        name_column = columns.index('NAME')
        code_column = columns.index('CODE')

        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': break # end of this table
            if entry[0][-1] <> u'\n':
                self.route_types_map[entry[code_column]] = self.net_route_types_map.get(entry[name_column], '3')

        if self.debug: print u'route_types_map: %s' % self.route_types_map



    def _write_agency(self, table_header_line): # reads form $BETREIBER
        f = open('agency.txt', 'w')
        writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(( u'agency_id', u'agency_name', u'agency_url', u'agency_timezone' )) # header

        columns = table_header_line.split(':')[1].split(';')

        id_column = columns.index('NR')
        name_column = columns.index('NAME')


        # read and write the entries
        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': break # end of this table
            if entry[0][-1] <> u'\n':
                writer.writerow((entry[id_column], entry[name_column], u'http://www.example.com', u'Europe/Berlin'))
        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('agency.txt')
        finally:
            zip_file.close()

        os.remove('agency.txt')


    def _process_vertices(self, table_header_line):
        self.vertices = {}
        columns = table_header_line.split(':')[1].split(';')

        id_column = columns.index('NR')
        x_coord_column = columns.index('XKOORD')
        y_coord_column = columns.index('YKOORD')

        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': break # end of this table

            if entry[0][-1] <> u'\n':
                lat, lon, h = coord_to_wgs84(self.from_proj, entry[x_coord_column], entry[y_coord_column])

                self.vertices[entry[id_column]] = ( str(lon), str(lat) )


    def _write_stations(self, table_header_line): # reads from $HALTESTELLE
        self.stations = []
        columns = table_header_line.split(':')[1].split(';')

        id_column = columns.index('NR')
        name_column = columns.index('NAME')
        x_coord_column = columns.index('XKOORD')
        y_coord_column = columns.index('YKOORD')


        # read and write the entries
        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': break # end of this table
            if entry[0][-1] <> u'\n':

                (lat, lon, h) = coord_to_wgs84(self.from_proj, entry[x_coord_column], entry[y_coord_column])
                stop_name = entry[name_column] if entry[name_column] else 'Unbenannter Stop'

                self.stations.append(( 'S'+entry[id_column], stop_name, str(lon), str(lat), '1', '' ))



    def _write_stops(self, table_header_line): # reads from $HALTESTELLENBEREICH
        f = open('stops.txt', 'w')
        writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(( u'stop_id', u'stop_name', u'stop_lat', u'stop_lon', u'location_type', u'parent_station' )) # header

        writer.writerows(self.stations)

        columns = table_header_line.split(':')[1].split(';')

        id_column = columns.index('NR')
        name_column = columns.index('NAME')
        vertex_id_column = columns.index('KNOTNR')
        station_id_column = columns.index('HSTNR')
        x_coord_column = columns.index('XKOORD')
        y_coord_column = columns.index('YKOORD')


        # read and write the entries
        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': break # end of this table

            if entry[0][-1] <> u'\n':
                (lat, lon, h) = coord_to_wgs84(self.from_proj, entry[x_coord_column], entry[y_coord_column])
                stop_name = entry[name_column] if entry[name_column] else 'Unbenannter Stop'

                writer.writerow(( entry[id_column], stop_name, str(lon), str(lat), '0', 'S'+entry[station_id_column] ))

        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('stops.txt')
        finally:
            zip_file.close()

        os.remove('stops.txt')

    def _process_stop_points(self, table_header_line): # rads from $HALTEPUNKT
        ''' Creates a dictionary [vertex_to_stop_mapper] that maps between vertex-IDs and stops-IDs.
        '''
        self.vertex_to_stop_mapper = {}
        self.stop_point_to_stop_mapper = {}

        columns = table_header_line.split(':')[1].split(';')

        vertex_id_column = columns.index('KNOTNR')
        stop_id_column = columns.index('HSTBERNR')
        stop_point_id_column = columns.index('NR')


        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': break # end of this table
            if entry[0][-1] <> u'\n':
                self.vertex_to_stop_mapper[entry[vertex_id_column]] = entry[stop_id_column]
                self.stop_point_to_stop_mapper[entry[stop_point_id_column]] = entry[stop_id_column]


    def _write_routes(self, table_header_line): # reads form $LINIE
        f = open('routes.txt', 'a')
        writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(( u'route_id', u'agency_id', u'route_short_name', u'route_long_name', u'route_type' ))# header

        columns = table_header_line.split(':')[1].split(';')
        for i,stuff in enumerate(columns): # find the position of the required columns
            if stuff == 'NAME':
                name_column = id_column = i
            elif stuff == 'VSYSCODE':
                route_type_column = i
            elif stuff == 'BETREIBERNR':
                agency_id_column = i

        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': break # end of this table
            if entry[0][-1] <> u'\n':
                writer.writerow(( entry[id_column],  '1', entry[name_column],'', self.route_types_map[entry[route_type_column]] ))
        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('routes.txt')
        finally:
            zip_file.close()

        os.remove('routes.txt')


    def _process_stop_id_mapper(self, table_header_line): # reads from $LINIENROUTENELEMENT
        ''' This method provides a dictionary to map from 'LRELEMINDEX' keys in the
            '$LINIENROUTENELEMENT' table to actual 'stop_id's.
            It contains a dictionary for each trip, which maps betwen indexes and stop_ids.
            See 'mapping net to gtf.txt' for further information.
        '''
        self.stop_id_mapper = {}
        self.shapes = {}

        columns = table_header_line.split(':')[1].split(';')
        for i,stuff in enumerate(columns): # find the position of the required columns
            if stuff == 'HPUNKTNR':
                stop_id_column = i
            elif stuff == 'LINNAME':
                route_id_column = i
            elif stuff == 'LINROUTENAME':
                lr_id_column = i # not actual the lr_id (see 'mapping net to gtf.txt')
            elif stuff == 'RICHTUNGCODE':
                direction_column = i
            elif stuff == 'INDEX':
                index_column = i
            elif stuff == 'ISTROUTENPUNKT':
                is_stop_column = i
            elif stuff == 'KNOTNR':
                vertex_id_column = i

        dict_lr_id = lre_dict = None
        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': # end of this table
                self.stop_id_mapper[dict_lr_id] = lre_dict
                break


            if entry[0][-1] <> u'\n':

                lr_id = '_'.join([entry[route_id_column], entry[lr_id_column], entry[direction_column]])

                # add entry to the shapes
                if lr_id not in self.shapes:
                    self.shapes[lr_id] = [ ( entry[index_column], entry[vertex_id_column] ) ]
                else:
                    self.shapes[lr_id].append(( entry[index_column], entry[vertex_id_column] ))

                if not entry[is_stop_column] == '0' and entry[stop_id_column]: # entry is a stop
                    if not lr_id == dict_lr_id:
                        self.stop_id_mapper[dict_lr_id] = lre_dict
                        lre_dict = {}
                        dict_lr_id = lr_id

                    lre_dict[entry[index_column]] = self.stop_point_to_stop_mapper[entry[stop_id_column]]

    def _process_raw_stop_times(self, table_header_line): # reads from $FAHRZEITPROFILELEMENT
        if self.debug: print 'processing raw stop times'
        self.fzp_stop_id_mapper = {}
        dict_fzp_id = fzpe_dict = None

        columns = table_header_line.split(':')[1].split(';')

        route_id_column = columns.index('LINNAME')
        lrname_column = columns.index('LINROUTENAME')
        direction_column = columns.index('RICHTUNGCODE')
        fzprofilname_column = columns.index('FZPROFILNAME')
        arrival_time_column = columns.index('ANKUNFT')
        departure_time_column = columns.index('ABFAHRT')
        stop_column = columns.index('LRELEMINDEX') # not stop_id (see 'mapping net to gtf.txt')
        stop_sequence_column = columns.index('INDEX')


        self.raw_stop_times = {}
        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': break # end of this table

            if entry[0][-1] <> u'\n':

                rst_id = '_'.join([entry[route_id_column], entry[lrname_column], entry[direction_column]])
                fzp_id = '_'.join([entry[route_id_column], entry[lrname_column], entry[direction_column], entry[fzprofilname_column]])

                if self.debug: print rst_id, fzp_id


                if not fzp_id == dict_fzp_id:
                    self.fzp_stop_id_mapper[dict_fzp_id] = fzpe_dict
                    fzpe_dict = {}
                    dict_fzp_id = fzp_id

                fzpe_dict[entry[stop_sequence_column]] = self.stop_id_mapper[rst_id][entry[stop_column]]


                lre = self.stop_id_mapper[rst_id]
##                fzpe = self.fzp_stop_id_mapper[fzp_id]
                if entry[stop_column] in lre:
                    stop_id = lre[entry[stop_column]]
                    if fzp_id in self.raw_stop_times:
                        self.raw_stop_times[fzp_id].append(( entry[stop_sequence_column], stop_id, entry[arrival_time_column], entry[departure_time_column] ))
                    else:
                        self.raw_stop_times[fzp_id] = [ ( entry[stop_sequence_column], stop_id, entry[arrival_time_column], entry[departure_time_column] ) ]

        if self.debug: print self.raw_stop_times

    def _write_stop_times_and_trips(self, table_header_line): # reads from $FAHRPLANFAHRT
        if self.debug: print 'writing stop_times.txt and trips.txt'

        st_file = open('stop_times.txt', 'w')
        st_writer = UnicodeWriter(st_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        st_writer.writerow(( u'lr_id', u'arrival_time', u'departure_time', u'stop_id', u'stop_sequence' )) # header

        t_file = open('trips.txt', 'w')
        t_writer = UnicodeWriter(t_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        t_writer.writerow(( u'route_id', u'service_id', u'lr_id', u'shape_id' ))

        columns = table_header_line.split(':')[1].split(';')

        lr_id_column = columns.index('NR')
        departure_column = columns.index('ABFAHRT')
        route_id_column = columns.index('LINNAME')
        lrname_column = columns.index('LINROUTENAME')
        direction_column = columns.index('RICHTUNGCODE')
        fzprofilname_column = columns.index('FZPROFILNAME')

        lr_id = 0
        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': break # end of this table

            if entry[0][-1] <> u'\n':
                lr_id += 1
                rst_id = '_'.join([entry[route_id_column], entry[lrname_column], entry[direction_column]])
                fzp_id = '_'.join([entry[route_id_column], entry[lrname_column], entry[direction_column], entry[fzprofilname_column]])
                fzp = self.raw_stop_times[fzp_id]
                if self.debug: print 'writing trip: %s' % lr_id

                t_writer.writerow(( entry[route_id_column], '1', str(lr_id), fzp_id if fzp_id in self.shapes else u'' )) # TODO: service_id

                for stop in fzp:
                    arrival_time = time_adder(stop[2], entry[departure_column])
                    departure_time = time_adder(stop[3], entry[departure_column])
                    st_writer.writerow(( str(lr_id), arrival_time, departure_time, stop[1], stop[0] ))

        st_file.close()
        t_file.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('stop_times.txt')
            zip_file.write('trips.txt')
        except:
            raise 'hallo'
        finally:
            zip_file.close()

        os.remove('stop_times.txt')
        os.remove('trips.txt')


    def _write_tranfers(self, table_header_line): # reads from $UEBERGANGSGEHZEITHSTBER
        if self.debug: print 'writing tranfers'
        f = open('transfers.txt', 'w')
        writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(( u'from_stop_id', u'to_stop_id', u'transfer_type', u'min_transfer_time' ))

        columns = table_header_line.split(':')[1].split(';')
        columns[-1] = columns[-1].split(self.eol)[0]

        from_stop_id_column = columns.index('VONHSTBERNR')
        to_stop_id_column = columns.index('NACHHSTBERNR')
        time_column = columns.index('ZEIT')


        while True:
            entry = self.input_file.next().split(';')
            if entry[0][0] == '*': break # end of this table
            if entry[0][-1] <> u'\n':
                transfer_time = entry[time_column][:-3] if len(self.eol) == 2 else entry[time_column][:-2]
                writer.writerow(( entry[from_stop_id_column], entry[to_stop_id_column], '2', transfer_time ))

        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('transfers.txt')
        finally:
            zip_file.close()

        os.remove('transfers.txt')


    def _write_shapes(self):
        f = open('shapes.txt', 'w')
        writer = UnicodeWriter(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(( u'shape_id', u'shape_pt_lat', u'shape_pt_lon', u'shape_pt_sequence' ))

        for s_id in self.shapes:
            for entry in self.shapes[s_id]:
                s_seq = entry[0]
                lat, lon  = self.vertices[entry[1]]

                writer.writerow(( s_id, lat, lon, s_seq ))

        f.close()

        try:
            zip_file = ZipFile(self.output_file, 'a')
            zip_file.write('shapes.txt')
        finally:
            zip_file.close()

        os.remove('shapes.txt')


    def _check_input(self):
        try:
            f = codecs.open(self.net_file, encoding='latin-1')
            # find out the end-of-line character
            l = f.next()
            if l[-2:] == '\r\n': # windows (CRLF
                self.eol = '\r\n'
            elif l[-1:] == '\n': # unix (LF)
                self.eol = '\n'
            else: # let's not expect more then LF and CRLF
                raise InvalidInputException()

            if self.debug: print u'line ending: %s' % list(self.eol) # in a list - so it won't cause a linebreak

            # TODO add more tests
        except:
            if self.debug: print u'ERROR: compatibility test failed!'
            raise InvalidInputException()



    def write_gtf(self):
        self.input_file = codecs.open(self.net_file, encoding='latin-1')

        try:
            while True:
                current_line = self.input_file.next()
                if current_line[0] == '$': # current_line is a table header
                    table_name = current_line.split(':')[0]
                    if table_name in self.table_to_func_mapper:
                        self.table_to_func_mapper[table_name](current_line)
        except StopIteration:
            self._write_shapes()
            if self.debug: print 'file completely read'
        #except:
         #   if self.debug: print 'error in write_gtf'
          #  raise InvalidInputException()
        finally:
            self.input_file.close()



    def __init__(self, net_file, output_file, debug=False, from_proj=Proj(init='epsg:4326'),
                       net_types_map={}, calendar_types=None):
        self.debug = debug
        self.net_file = net_file
        self.output_file = output_file if output_file[-4:] == '.zip' else output_file+'.zip'
        self.from_proj = from_proj
        self.net_route_types_map = net_types_map
        self.calendar_types = calendar_types
        self.table_to_func_mapper = { '$VERKEHRSTAG':self._write_calendar,
                                      '$VSYS':self._process_route_types,
                                      '$BETREIBER':self._write_agency,
                                      '$HALTESTELLE': self._write_stations,
                                      '$HALTESTELLENBEREICH': self._write_stops,
                                      '$HALTEPUNKT':self._process_stop_points,
                                      '$LINIE':self._write_routes,
                                      '$LINIENROUTENELEMENT':self._process_stop_id_mapper,
##                                      '$FAHRZEITPROFILELEMENT':self._process_fzp_stop_id_mapper,
                                      '$FAHRZEITPROFILELEMENT':self._process_raw_stop_times,
                                      '$FZGFAHRT':self._write_stop_times_and_trips,
                                      '$UEBERGANGSGEHZEITHSTBER':self._write_tranfers,
                                      '$KNOTEN':self._process_vertices }


        self._check_input()
        eliminate_blank_lines(self.net_file, self.eol)




def main():
    from optparse import OptionParser
    usage = """usage: python netToGtf.py [options] input_file output_file"""
    parser = OptionParser(usage=usage)
    parser.add_option("-d", "--debug", action="store_true", help="print debug information", dest="debug", default=False)
    parser.add_option("-p", "--proj_code", help="set the corresponding proj-init code for the coordinates inside the .net file. See http://code.google.com/p/pyproj/source/browse/trunk/lib/pyproj/data/epsg for all possiblities. If option is not set WGS84 will be used.",
                        dest="proj_code", default="4326")

    (options, args) = parser.parse_args()

    if len(args) < 2 or len(args) > 4:
        parser.print_help()
        exit(-1)

    try:
        ntg = NetToGtf(args[0], args[1], debug=options.debug, net_types_map={},
                           calendar_types=None, from_proj=Proj(init='epsg:'+options.proj_code))

        ntg.write_gtf()
    except InvalidInputException:
        print u"Error: looks like the input file is not valid!\n"
        parser.print_help()
        exit(-1)
    #except:
        print u"something went wrong\n"
        parser.print_help()
        exit(-1)

##netfile = r'D:\temp\MP\Fahrplandaten_MSP.net'
##outfile = r'D:\temp\MP\test'
##ntg = NetToGtf(netfile, outfile, debug=False, net_types_map={},
##                   calendar_types=None, from_proj=Proj(init='epsg:4326'))
##
##ntg.write_gtf()

if __name__ == '__main__': main()