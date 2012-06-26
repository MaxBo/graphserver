import psycopg2
import os
try:
    import json
except ImportError:
    import simplejson as json
import sys
import xml.sax
import binascii
from graphserver.vincenty import vincenty
from struct import pack, unpack
from rtree import Rtree

def cons(ary):
    for i in range(len(ary)-1):
        yield (ary[i], ary[i+1])

def pack_coords(coords):
    return binascii.b2a_base64( "".join([pack( "ff", *coord ) for coord in coords]) )

def unpack_coords(str):
    bin = binascii.a2b_base64( str )
    return [unpack( "ff", bin[i:i+8] ) for i in range(0, len(bin), 8)]

class Node:
    def __init__(self, id, lon, lat):
        self.id = id
        self.lon = lon
        self.lat = lat
        self.tags = {}

    def __repr__(self):
        return "<Node id='%s' (%s, %s) n_tags=%d>"%(self.id, self.lon, self.lat, len(self.tags))

class Way:
    def __init__(self, id):
        self.id = id
        self.nd_ids = []
        self.tags = {}

    def __repr__(self):
        return "<Way id='%s' n_nds=%d n_tags=%d>"%(self.id, len(self.nd_ids), len(self.tags))

class WayRecord:
    def __init__(self, id, tags, nds):
        self.id = id

        if type(tags)==unicode:
            self.tags_str = tags
            self.tags_cache = None
        else:
            self.tags_cache = tags
            self.tags_str = None

        if type(nds)==unicode:
            self.nds_str = nds
            self.nds_cache = None
        else:
            self.nds_cache = nds
            self.nds_str = None

    @property
    def tags(self):
        self.tags_cache = self.tags_cache or json.loads(self.tags_str)
        return self.tags_cache

    @property
    def nds(self):
        self.nds_cache = self.nds_cache or json.loads(self.nds_str)
        return [ nd.replace('"','').replace('[','').replace(']','') for nd in self.nds_cache.split(', ')]

    def __repr__(self):
        return "<WayRecord id='%s'>"%self.id

class OSMDB:
    def __init__(self, psql_conn_string, overwrite=False, rtree_index=True):
        self.conn = psycopg2.connect(psql_conn_string)


        c = self.conn.cursor()
        c.execute("select tablename from pg_tables where schemaname='public'" )
        tables = c.fetchall()

        for t in ( 'osm_nodes', 'osm_ways' ):
            if (t,) not in tables:
                overwrite = True

        c.close()

        if overwrite:
            self.setup()

        if rtree_index:
            self.index = Rtree()
            self.index_endnodes()
        else:
            self.index = None

    def setup(self):
        c = self.conn.cursor()

        c.execute( "DROP TABLE IF EXISTS osm_nodes CASCADE" )
        c.execute( "DROP TABLE IF EXISTS osm_ways CASCADE" )

        c.execute( "CREATE TABLE osm_nodes (id TEXT PRIMARY KEY, tags TEXT, lat FLOAT, lon FLOAT, endnode_refs INTEGER DEFAULT 1)" )
        c.execute( "CREATE TABLE osm_ways (id TEXT PRIMARY KEY, tags TEXT, nds TEXT)" )

        self.conn.commit()
        c.close()

    def create_indexes(self):
        c = self.conn.cursor()

        c.execute( "CREATE INDEX nodes_lon ON osm_nodes (lon)" )
        c.execute( "CREATE INDEX nodes_lat ON osm_nodes (lat)" )

        self.conn.commit()
        c.close()

    def populate(self, osm_filename, dryrun=False, accept=lambda tags: True, reporter=None, create_indexes=True):
        print "importing %s osm from XML to psql database" % osm_filename

        c = self.conn.cursor()

        self.n_nodes = 0
        self.n_ways = 0

        superself = self

        class OSMHandler(xml.sax.ContentHandler):
            @classmethod
            def setDocumentLocator(self,loc):
                pass

            @classmethod
            def startDocument(self):
                pass

            @classmethod
            def endDocument(self):
                pass

            @classmethod
            def startElement(self, name, attrs):
                if name=='node':
                    self.currElem = Node(attrs['id'], float(attrs['lon']), float(attrs['lat']))
                elif name=='way':
                    self.currElem = Way(attrs['id'])
                elif name=='tag':
                    self.currElem.tags[attrs['k']] = attrs['v']
                elif name=='nd':
                    self.currElem.nd_ids.append( attrs['ref'] )

            @classmethod
            def endElement(self,name):
                if name=='node':
                    if superself.n_nodes%5000==0:
                        print "node %d"%superself.n_nodes
                    superself.n_nodes += 1
                    if not dryrun: superself.add_node( self.currElem, c )
                elif name=='way':
                    if superself.n_ways%5000==0:
                        print "way %d"%superself.n_ways
                    superself.n_ways += 1
                    if not dryrun and accept(self.currElem.tags): superself.add_way( self.currElem, c )

            @classmethod
            def characters(self, chars):
                pass

        xml.sax.parse(osm_filename, OSMHandler)

        self.conn.commit()
        c.close()

        if not dryrun and create_indexes:
            print "indexing primary tables...",
            self.create_indexes()

        print "done"

    def set_endnode_ref_counts( self ):
        """Populate ways.endnode_refs. Necessary for splitting ways into single-edge sub-ways"""

        print "counting end-node references to find way split-points"

        c = self.conn.cursor()

        endnode_ref_counts = {}

        c.execute( "SELECT nds from osm_ways" )

        print "...counting"
        for i, (nds_str,) in enumerate(c):
            if i%5000==0:
                print i

            nds = json.loads( nds_str )
            for nd in nds:
                endnode_ref_counts[ nd ] = endnode_ref_counts.get( nd, 0 )+1

        print "...updating nodes table"
        for i, (node_id, ref_count) in enumerate(endnode_ref_counts.items()):
            if i%5000==0:
                print i

            if ref_count > 1:
                c.execute( "UPDATE osm_nodes SET endnode_refs=%s WHERE id=%s", (ref_count, node_id) )

        self.conn.commit()
        c.close()

    def index_endnodes( self ):
        print "indexing endpoint nodes into rtree"

        c = self.conn.cursor()

        #TODO index endnodes if they're at the end of oneways - which only have one way ref, but are still endnodes
        c.execute( "SELECT id, lat, lon FROM osm_nodes WHERE endnode_refs > 1" )

        for id, lat, lon in c:
            self.index.add( int(id), (lon, lat, lon, lat) )

        c.close()

    def create_and_populate_edges_table( self, tolerant=False ):
        self.set_endnode_ref_counts()
        self.index_endnodes()

        print "splitting ways and inserting into edge table"

        c = self.conn.cursor()

        c.execute( "DROP TABLE IF EXISTS osm_edges" )
        c.execute( """CREATE TABLE osm_edges (id TEXT PRIMARY KEY,
                                              parent_id TEXT REFERENCES osm_ways ON DELETE CASCADE,
                                              start_nd TEXT REFERENCES osm_nodes ON DELETE CASCADE,
                                              end_nd TEXT REFERENCES osm_nodes ON DELETE CASCADE,
                                              dist FLOAT,
                                              geom TEXT)""" )

        for i, way in enumerate(self.ways()):
            try:
                if i%5000==0:
                    print i

                subways = []
                curr_subway = [ way.nds[0] ] # add first node to the current subway

                for nd in way.nds[1:-1]:     # for every internal node of the way
                    curr_subway.append( nd )
                    if self.node(nd)[4] > 1: # node reference count is greater than one, node is shared by two ways
                        subways.append( curr_subway )
                        curr_subway = [ nd ]
                curr_subway.append( way.nds[-1] ) # add the last node to the current subway, and store the subway
                subways.append( curr_subway );

                #insert into edge table
                for i, subway in enumerate(subways):
                    coords = [(lambda x:(x[3],x[2]))(self.node(nd)) for nd in subway]
                    packt = pack_coords( coords )
                    dist = sum([vincenty(lat1, lng1, lat2, lng2) for (lng1, lat1), (lng2, lat2) in cons(coords)])
                    c.execute( "INSERT INTO osm_edges VALUES (%s, %s, %s, %s, %s, %s)",
                                                                             ( "%s-%s"%(way.id, i),
                                                                               way.id,
                                                                               subway[0],
                                                                               subway[-1],
                                                                               dist,
                                                                               packt) )
            except IndexError:
                if tolerant:
                    continue
                else:
                    raise

        print "indexing edges...",
        c.execute( "CREATE INDEX osm_edges_parent_id ON osm_edges (parent_id)" )
        c.execute( "CREATE INDEX osm_edges_start_nd ON osm_edges (start_nd)" )
        c.execute( "CREATE INDEX osm_edges_end_nd ON osm_edges (end_nd)" )
        print "done"

        self.conn.commit()
        c.close()

    def edge(self, id):
        c = self.conn.cursor()

        c.execute( "SELECT osm_edges.*, osm_ways.tags FROM osm_edges, osm_ways WHERE osm_ways.id = osm_edges.parent_id AND osm_edges.id = %s", (id,) )

        try:
            ret = c.next()
            way_id, parent_id, from_nd, to_nd, dist, geom, tags = ret
            return (way_id, parent_id, from_nd, to_nd, dist, unpack_coords( geom ), json.loads(tags))
        except StopIteration:
            c.close()
            raise IndexError( "Database does not have an edge with id '%s'"%id )

        c.close()
        return ret

    def edges(self):
        c = self.conn.cursor()

        c.execute( "SELECT osm_edges.*, osm_ways.tags FROM osm_edges, osm_ways WHERE osm_ways.id = osm_edges.parent_id" )

        for way_id, parent_id, from_nd, to_nd, dist, geom, tags in c:
            yield (way_id, parent_id, from_nd, to_nd, dist, unpack_coords(geom), json.loads(tags))

        c.close()


    def add_way( self, way, curs=None ):
        if curs is None:
            curs = self.conn.cursor()
            close_cursor = True
        else:
            close_cursor = False

        curs.execute("INSERT INTO osm_ways (id, tags, nds) VALUES (%s, %s, %s)", (way.id, json.dumps(way.tags), json.dumps(way.nd_ids) ))

        if close_cursor:
            self.conn.commit()
            curs.close()

    def add_node( self, node, curs=None ):
        if curs is None:
            curs = self.conn.cursor()
            close_cursor = True
        else:
            close_cursor = False

        curs.execute("INSERT INTO osm_nodes (id, tags, lat, lon) VALUES (%s, %s, %s, %s)", ( node.id, json.dumps(node.tags), node.lat, node.lon ) )

        if close_cursor:
            self.conn.commit()
            curs.close()

    def nodes(self):
        c = self.conn.cursor()

        c.execute( "SELECT * FROM osm_nodes" )

        for node_row in c:
            yield node_row

        c.close()

    def node(self, id):
        c = self.conn.cursor()

        c.execute( "SELECT * FROM osm_nodes WHERE id = %s", (id,) )

        try:
            ret = c.next()
        except StopIteration:
            c.close()
            raise IndexError( "Database does not have node with id '%s'"%id )

        c.close()
        return ret

    def nearest_node(self, lat, lon, range=0.005):
        c = self.conn.cursor()

        if self.index:
            #print "YOUR'RE USING THE INDEX"
            id = list(self.index.nearest( (lon, lat), 1 ))[0]
            #print "THE ID IS %d"%id
            c.execute( "SELECT id, lat, lon FROM osm_nodes WHERE id = %s", (id,) )
        else:
            c.execute( "SELECT id, lat, lon FROM osm_nodes WHERE endnode_refs > 1 AND lat > %s AND lat < %s AND lon > %s AND lon < %s", (lat-range, lat+range, lon-range, lon+range) )

        dists = [(nid, nlat, nlon, ((nlat-lat)**2+(nlon-lon)**2)**0.5) for nid, nlat, nlon in c]

        if len(dists)==0:
            return (None, None, None, None)

        return min( dists, key = lambda x:x[3] )

    def nearest_of( self, lat, lon, nodes ):
        c = self.conn.cursor()

        c.execute( "SELECT id, lat, lon FROM osm_nodes WHERE id IN (%s)"%",".join([str(x) for x in nodes]) )

        dists = [(nid, nlat, nlon, ((nlat-lat)**2+(nlon-lon)**2)**0.5) for nid, nlat, nlon in c]

        if len(dists)==0:
            return (None, None, None, None)

        return min( dists, key = lambda x:x[3] )

    def way(self, id):
        c = self.conn.cursor()

        c.execute( "SELECT id, tags, nds FROM osm_ways WHERE id = %s", (id,) )

        try:
          id, tags_str, nds_str = c.next()
          ret = WayRecord(id, tags_str, nds_str)
        except StopIteration:
          raise Exception( "OSMDB has no way with id '%s'"%id )
        finally:
          c.close()

        return ret

    def way_nds(self, id):
        c = self.conn.cursor()
        c.execute( "SELECT nds FROM osm_ways WHERE id = %s", (id,) )

        (nds_str,) = c.next()
        c.close()

        return json.loads( nds_str )

    def ways(self):
        c = self.conn.cursor()

        c.execute( "SELECT id, tags, nds FROM osm_ways" )

        for id, tags_str, nds_str in c:
            yield WayRecord( id, tags_str, nds_str )

        c.close()

    def count_ways(self):
        c = self.conn.cursor()

        c.execute( "SELECT count(*) FROM osm_ways" )
        ret = c.next()[0]

        c.close()

        return ret

    def count_edges(self):
        c = self.conn.cursor()

        c.execute( "SELECT count(*) FROM osm_edges" )
        ret = c.next()[0]

        c.close()

        return ret

    def delete_way(self, id):
        c = self.conn.cursor()

        c.execute("DELETE FROM osm_ways WHERE id = %s", (id,))

        c.close()

    def bounds(self):
        c = self.conn.cursor()
        c.execute( "SELECT min(lon), min(lat), max(lon), max(lat) FROM osm_nodes" )

        ret = c.next()
        c.close()
        return ret

    def execute(self,sql,args=None):
        c = self.conn.cursor()
        if args:
            c.execute(sql,args)
            for row in c:
                yield row
        else:
            c.execute(sql)
            for row in c:
                yield row
        c.close()

    def cursor(self):
        return self.conn.cursor()

def test_wayrecord():
    wr = WayRecord( "1", {'highway':'bumpkis'}, ['1','2','3'] )
    assert wr.id == "1"
    assert wr.tags == {'highway':'bumpkis'}
    assert wr.nds == ['1','2','3']

    wr = WayRecord( "1", "{\"highway\":\"bumpkis\"}", "[\"1\",\"2\",\"3\"]" )
    assert wr.id == "1"
    assert wr.tags == {'highway':'bumpkis'}
    assert wr.nds == ['1','2','3']

def osm_to_osmdb(osm_filenames, db_conn_string, tolerant=False, skipload=False):
    osmdb = OSMDB( db_conn_string, overwrite=True )

    if isinstance(osm_filenames, basestring):
        osm_filenames = (osm_filenames, )

    for osm_filename in osm_filenames:
        if not skipload:
            osmdb.populate( osm_filename, accept=lambda tags: 'highway' in tags, reporter=sys.stdout, create_indexes=False )

    if not skipload:
        print "indexing primary tables...",
        osmdb.create_indexes()

    osmdb.create_and_populate_edges_table(tolerant)
    if osmdb.count_edges() == 0:
        print "WARNING: osmdb has no edges!"

    return osmdb

from optparse import OptionParser
def main():
    from sys import argv

    parser = OptionParser(usage="%prog [options] osm_filename [osm_filename ...] osmdb_filename")
    parser.add_option( "-t", "--tolerant", dest="tolerant",
                       action="store_true" )
    parser.add_option( "-d", "--dryrun", dest="dryrun", help="Just read the OSM file; don't copy anything to a database", action="store_true" )

    (options, args) = parser.parse_args()

    if len(args) < 2:
        parser.error("incorrect number of arguments")
    osmdb_filename = args.pop()

    osm_to_osmdb(args, osmdb_filename, options.tolerant, options.dryrun)

if __name__=='__main__':
    main()
