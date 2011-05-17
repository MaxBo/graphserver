#! /usr/bin/env python

from zipfile import ZipFile
from optparse import OptionParser
from subprocess import Popen

from graphserver_tools.utils import utf8csv

def read_lat_lon_from_stops(zipped_file):
    try:
        reader = utf8csv.UnicodeReader(zipped_file.open("stops.txt"))
    except:
        print "ERROR: bad gtfs-feed - could not open stops.txt"
        exit(-1)

    lats = []
    lons = []

    header = reader.next()
    lat_index = header.index(u'stop_lat')
    lon_index = header.index(u'stop_lon')

    for l in reader:
        if not l: continue

        lats.append(float(l[lat_index]))
        lons.append(float(l[lon_index]))

    return lats, lons


def crop_osm(gtfs_feed, input_osm, output_osm):
    lats, lons = read_lat_lon_from_stops(gtfs_feed)

    left = min(lons)
    bottom = min(lats)
    right = max(lons)
    top = max(lats)

    print '\ncalculated smallest rectangle around gtfs-feed:'
    print '\tleft: %f' % left
    print '\tbottom: %f' % bottom
    print '\tright: %f' % right
    print '\ttop: %f\n' % top

    args = [ 'osmosis', '--read-xml', input_osm, '--bounding-box', 'completeWays=yes',
             'left='+str(left), 'bottom='+str(bottom), 'right='+str(right),
             'top='+str(top), '--write-xml', output_osm ]

    Popen(args).communicate()


def main():
    parser = OptionParser("""usage: python netToGtf.py gtfs_file input_osm [output_osm]""")

    (options, args) = parser.parse_args()

    if len(args) < 2 or len(args) > 3:
        parser.print_help()
        exit(-1)

    if len(args) == 2:
        output_osm = args[1][:-4] + '-cropped.osm'
    else:
        output_osm = args[2]


    crop_osm(ZipFile(args[0]), args[1], output_osm)


if __name__ == '__main__': main()