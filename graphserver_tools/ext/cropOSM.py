#! /usr/bin/env python

from zipfile import zipfile
from optparse import OptionParser
from subprocess import Popen

def read_lat_lon_from_stops(zipped_file):
    try:
        stops_file = zipped_file.open("stops.txt")
    except:
        print "ERROR: bad gtfs-feed - could not open stops.txt"
        exit(-1)

    lats = []
    lons = []

    lines = [l for l in stops_file]

    lat_index = l[0].split(',').index('stop_lat')
    lon_index = l[0].split(',').index('stop_lon')

    for l in lines:
        lats.append(float(l[lat_index]))
        lons.append(float(l[lon_index]))

    return lats, lons


def crop_osm(gtfs_feed, input_osm, output_osm):
    lats, lons = read_lat_lon_from_stops(gtfs_feed)

    left = min(lons)
    bottom = min(lats)
    right = max(lons)
    top = max(lats)

    print 'calculated smallest rectangle around gtfs-feed:'
    print 'left: %f' % left
    print 'bottom: %f' % bottom
    print 'right: %f' % right
    print 'top: %f' % top

    args = [ 'osmosis', '--read-xml', input_osm, '--bounding-box', 'completeWays=yes',
             'left='+string(left), 'bottom='+string(bottom), 'right='+string(right),
             'top='+string(top), '--write_xml', output_osm ]

    Popen(args).communicate()


def main():
    parser = OptionParser("""usage: python netToGtf.py gtfs_file input_osm [output_osm]""")

    (options, args) = parser.parse_args()

    if len(args) < 2 or len(args) > 3:
        parser.print_help()
        exit(-1)

    if len(args) == 2:
        output_osm = input_osm.split('.')[:-1] + 'cropped.osm'
    else:
        output_osm = args[2]


    crop_osm(args[0], args[1], output_osm)


if __name__ == '__main__': main()