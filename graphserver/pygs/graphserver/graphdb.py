import os
import psycopg2
import cPickle
from graphserver.core import State, Graph, Combination
from graphserver import core
from sys import argv
import sys

class GraphDatabase:

    def __init__(self, psql_conn_string, overwrite=False):

        self.conn = psycopg2.connect(psql_conn_string)

        c = self.conn.cursor()
        c.execute("select tablename from pg_tables where schemaname='public'" )
        tables = c.fetchall()

        for t in ( 'graph_vertices', 'graph_payloads', 'graph_edges', 'graph_resources' ):
            if (t,) not in tables:
                overwrite = True

        if overwrite:
            self.setup()

        # prepared inserts for better performance
        c.execute( """PREPARE preparedGraphVertexInsert ( TEXT ) AS
                      INSERT INTO graph_vertices ( label ) VALUES ( $1 )""" )

        c.execute( """PREPARE preparedGraphEdgeInsert ( TEXT, TEXT, INTEGER ) AS
                      INSERT INTO graph_edges ( vertex1, vertex2, epid ) VALUES ( $1, $2, $3 )""" )

        c.execute( """PREPARE preparedPayloadInsert ( TEXT, TEXT ) AS
                      INSERT INTO graph_payloads ( type, state ) VALUES ( $1, $2) RETURNING id""")

        c.execute( """PREPARE preparedResourcesInsert ( TEXT, TEXT ) AS
                      INSERT INTO graph_resources ( name, image ) VALUES ( $1, $2 )""")

        c.close()

        self.resources_cache = {}
        self.payloads_cache = {}

    def setup(self):
        c = self.conn.cursor()
        c.execute( "DROP TABLE IF EXISTS graph_vertices CASCADE" )
        c.execute( "DROP TABLE IF EXISTS graph_payloads CASCADE" )
        c.execute( "DROP TABLE IF EXISTS graph_edges CASCADE" )
        c.execute( "DROP TABLE IF EXISTS graph_resources CASCADE" )

        # fast version - duplicate database entries
        c.execute( "CREATE TABLE graph_vertices (label TEXT )" )
        c.execute( "CREATE TABLE graph_resources (name TEXT, image TEXT)" )
        c.execute( "CREATE TABLE graph_payloads (id SERIAL, type TEXT, state TEXT)" )
        c.execute( "CREATE TABLE graph_edges (vertex1 TEXT , vertex2 TEXT, epid INTEGER)" )
        
        c.execute( "CREATE INDEX graph_edges_vertex1_idx ON graph_edges (vertex1)" )
        c.execute( "CREATE INDEX graph_edges_vertex2_idx ON graph_edges (vertex2)" )

        # much slower, but consistent version
        '''c.execute( "CREATE TABLE graph_vertices (label TEXT PRIMARY KEY)" )
        c.execute( "CREATE TABLE graph_resources (name TEXT PRIMARY KEY, image TEXT)" )
        c.execute( "CREATE TABLE graph_payloads (id SERIAL PRIMARY KEY, type TEXT, state TEXT)" )
        c.execute( "CREATE TABLE graph_edges (vertex1 TEXT REFERENCES graph_vertices, vertex2 TEXT REFERENCES graph_vertices, epid INTEGER REFERENCES graph_payloads)" )

        c.execute( """CREATE OR REPLACE RULE ignore_duplicate_graph_vertices
                      AS ON INSERT TO graph_vertices
                      WHERE label IN ( SELECT label FROM graph_vertices)
                      DO INSTEAD NOTHING""" )
        '''
        c.execute( """CREATE OR REPLACE RULE ignore_duplicate_graph_resources
                      AS ON INSERT TO graph_resources
                      WHERE name IN ( SELECT name FROM graph_resources)
                      DO INSTEAD NOTHING""" )


        self.conn.commit()
        
    def put_edge_payload(self, edgepayload, cc):

        if edgepayload.__class__ == Combination:
            edge_state = []

            for component in edgepayload.components:
                rowid = self.put_edge_payload( component, cc )

            edge_state.append( rowid )
        else:
            edge_state = edgepayload.__getstate__()


        cc.execute( "EXECUTE preparedPayloadInsert (%s, %s)", ( cPickle.dumps( edgepayload.__class__ ), cPickle.dumps( edge_state ) ) )

        return cc.fetchone()

    def get_edge_payload(self, id):
        queryresult = list(self.execute( "SELECT id, type, state FROM graph_payloads WHERE id=%s", (id,) ))
        if len(queryresult)==0:
            return None

        id, type, state = queryresult[0]

        if id in self.payloads_cache:
            return self.payloads_cache[id]

        typeclass = cPickle.loads( str(type) )
        ret = typeclass.reconstitute( cPickle.loads( str(state) ), self )
        ret.external_id = int(id)
        self.payloads_cache[id] = ret
        return ret

    def populate(self, graph, reporter=None):
        c = self.conn.cursor()

        n = len(graph.vertices)
        nseg = max(n,100)
        for i, vv in enumerate( graph.vertices ):
            if reporter and i%(nseg//100)==0: reporter.write( "%d/%d vertices dumped\n"%(i,n) )

            c.execute( "EXECUTE preparedGraphVertexInsert (%s)", (vv.label,) )
            for ee in vv.outgoing:
                epid = self.put_edge_payload( ee.payload, c )

                c.execute( "EXECUTE  preparedGraphEdgeInsert (%s, %s, %s)", (ee.from_v.label, ee.to_v.label, epid) )

                if hasattr(ee.payload, "__resources__"):
                    for name, resource in ee.payload.__resources__():

                        self.store( name, resource, c )

        self.conn.commit()
        c.close()

        self.index()

    def get_cursor(self):
        return self.conn.cursor()

    def commit(self):
        self.conn.commit()

    def add_vertex(self, vertex_label, outside_c=None):
        c = outside_c or self.conn.cursor()

        c.execute( "EXECUTE preparedGraphVertexInsert (%s)", (vertex_label,) )

        if outside_c is None:
            self.conn.commit()
            c.close()

    def remove_edge( self, oid, outside_c=None ):
        c = outside_c or self.conn.cursor()

        c.execute( "DELETE FROM graph_edges WHERE oid=%s", (oid,) )

        if outside_c is None:
            self.conn.commit()
            c.close()

    def add_edge(self, from_v_label, to_v_label, payload, outside_c=None):
        c = outside_c or self.conn.cursor()

        epid = self.put_edge_payload( payload, c )[0]
        c.execute( "EXECUTE  preparedGraphEdgeInsert (%s, %s, %s)", (from_v_label, to_v_label, epid) )

        if hasattr(payload, "__resources__"):
            for name, resource in payload.__resources__():
                self.store( name, resource )

        if outside_c is None:
            self.conn.commit()
            c.close()

    def execute(self, query, args=None):

        c = self.conn.cursor()

        if args:
            c.execute( query, args )
        else:
            c.execute( query )

        for record in c:
            yield record
        c.close()

    def all_vertex_labels(self):
        for vertex_label, in self.execute( "SELECT DISTINCT label FROM (SELECT vertex1 AS label FROM graph_edges UNION SELECT vertex2 AS label FROM graph_edges) AS label" ):
            yield vertex_label

    def all_edges(self):
        for vertex1, vertex2, epid in self.execute( "SELECT vertex1, vertex2, epid FROM graph_edges" ):
            ep = self.get_edge_payload( epid )

            yield vertex1, vertex2, ep

    def all_outgoing(self, vertex1_label):
        for vertex1, vertex2, epid in self.execute( "SELECT vertex1, vertex2, epid FROM graph_edges WHERE vertex1=%s", (vertex1_label,) ):
            yield vertex1, vertex2, self.get_edge_payload( epid )

    def all_incoming(self, vertex2_label):
        for vertex1, vertex2, epid in self.execute( "SELECT vertex1, vertex2, epid FROM graph_edges WHERE vertex2=%s", (vertex2_label,) ):
            yield vertex1, vertex2, self.get_edge_payload( epid )


    def store(self, name, obj, c=None):
        cc = self.conn.cursor() if c is None else c

        cc.execute( "INSERT INTO graph_resources VALUES (%s, %s)", (name, cPickle.dumps( obj )) )

        if not c:
            self.conn.commit()
            cc.close()

    def resolve(self, name):
        if name in self.resources_cache:
            return self.resources_cache[name]
        else:

            image = list(self.execute( "SELECT image FROM graph_resources WHERE name = %s", (str(name),) ))[0][0]
            resource = cPickle.loads( str(image) )
            self.resources_cache[name] = resource
            return resource

    def resources(self):
        for name, image in self.execute( "SELECT name, image from graph_resources" ):
            yield name, cPickle.loads( str(image) )

    def index(self):
        c = self.conn.cursor()
        c.execute( "CREATE INDEX graph_vertices_label ON graph_vertices (label)" )
        c.execute( "CREATE INDEX ep_ids ON graph_payloads (id)" )
        self.conn.commit()
        c.close()

    def num_vertices(self):
        return list(self.execute( "SELECT count(*) from graph_vertices" ))[0][0]

    def num_edges(self):
        return list(self.execute( "SELECT count(*) from graph_edges" ))[0][0]

    def incarnate(self, reporter=sys.stdout):
        g = Graph()
        num_vertices = self.num_vertices()

        for i, vertex_label in enumerate( self.all_vertex_labels() ):
            if reporter and i%5000==0:
                reporter.write("\r%d/%d vertices"%(i,num_vertices) )
                reporter.flush()
            g.add_vertex( vertex_label )

        if reporter: reporter.write("\rLoaded %d vertices %s\n" % (num_vertices, " "*10))

        num_edges = self.num_edges()
        for i, (vertex1, vertex2, edgetype) in enumerate( self.all_edges() ):
            if i%5000==0:
                reporter.write("\r%d/%d edges"%(i,num_edges) )
                reporter.flush()
            g.add_edge( vertex1, vertex2, edgetype )
        if reporter: reporter.write("\rLoaded %d edges %s\n" % (num_edges, " "*10))

        return g


def main():
    if len(argv) < 2:
        print "usage: python graphdb.py [vertex1, [vertex2]]"
        return

    graphdb_filename = argv[1]
    graphdb = GraphDatabase( graphdb_filename )

    if len(argv) == 2:
        print "vertices:"
        for vertex_label in sorted( graphdb.all_vertex_labels() ):
            print vertex_label
        print "resources:"
        for name, resource in graphdb.resources():
            print name, resource
        return

    vertex1 = argv[2]
    for vertex1, vertex2, edgetype in graphdb.all_outgoing( vertex1 ):
        print "%s -> %s\n\t%s"%(vertex1, vertex2, repr(edgetype))

        if len(argv) == 4:
            s0 = State(1,int(argv[3]))
            print "\t"+str(edgetype.walk( s0 ))

if __name__=='__main__':
    main()
