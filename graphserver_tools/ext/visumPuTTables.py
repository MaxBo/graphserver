#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller, Max Bohnet
# 21.10.2011
# Gertz Gutsche Rümenapp Gbr

import psycopg2
import threading

class VisumPuTTables(object):

    def transform(self):
        ''' Converts the feed associated with this object into a data for a visum database.
        '''

        print 'creating internal data structures'
        self._getValidUnterlinien()


        print 'converting'
        self._processBetreiber()
        self._processVsysset()

        threads = []

        for m in (  self._processKnoten,
                    self._processHstHstBereichHstPunkt,
                    self._processLinieRouteElement,
                    self._processFahrzeitprofil,
                    self._processFahrzeitprofilelement,
                    self._processFahrzeugfahrt,
                    self._processZwischenpunkte
                 ):

            t = threading.Thread(target=m)
            t.start()

            threads.append(t)


        for t in threads:
            t.join()

    def _createDbTables(self):

        connection = psycopg2.connect(self.db_connect_string)
        cursor = connection.cursor()


        cursor.execute('DROP TABLE IF EXISTS "BETREIBER" CASCADE')
        cursor.execute('''CREATE TABLE "BETREIBER"
                                (   "NR" integer,
                                    "NAME" varchar(255),
                                    "KOSTENSATZ1" float,
                                    "KOSTENSATZ2" float,
                                    "KOSTENSATZ3" float,
                                    PRIMARY KEY ("NR")
                                )''')

        cursor.execute('DROP TABLE IF EXISTS "FAHRZEITPROFIL" CASCADE')
        cursor.execute('''CREATE TABLE "FAHRZEITPROFIL"
                                (   "LINNAME" varchar(255),
                                    "LINROUTENAME" varchar(255),
                                    "RICHTUNGCODE" varchar(255),
                                    "NAME" varchar(255)
                                )''')

        cursor.execute('DROP TABLE IF EXISTS "FAHRZEITPROFILELEMENT" CASCADE')
        cursor.execute('''CREATE TABLE "FAHRZEITPROFILELEMENT"
                                (   "LINNAME" varchar(255),
                                    "LINROUTENAME" varchar(255),
                                    "RICHTUNGCODE" varchar(255),
                                    "FZPROFILNAME" varchar(255),
                                    "INDEX" integer,
                                    "LRELEMINDEX" integer,
                                    "AUS" integer,
                                    "EIN" integer,
                                    "ANKUNFT" timestamp,
                                    "ABFAHRT" timestamp
                                )''')

        cursor.execute('DROP TABLE IF EXISTS "FAHRPLANFAHRT" CASCADE')
        cursor.execute('''CREATE TABLE "FAHRPLANFAHRT"
                            (   "NR" integer,
                                "NAME" varchar(255),
                                "ABFAHRT" timestamp,
                                "LINNAME" varchar(255),
                                "LINROUTENAME" varchar(255),
                                "RICHTUNGCODE" varchar(255),
                                "FZPROFILNAME" varchar(255),
                                "VONFZPELEMINDEX" integer,
                                "NACHFZPELEMINDEX" integer
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "HALTESTELLE" CASCADE')
        cursor.execute('''CREATE TABLE "HALTESTELLE"
                            (   "NR" integer,
                                "CODE" varchar(255),
                                "NAME" varchar(255),
                                "TYPNR" integer,
                                "XKOORD" float NOT NULL,
                                "YKOORD" float NOT NULL,
                                PRIMARY KEY ("NR")
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "HALTESTELLENBEREICH" CASCADE')
        cursor.execute('''CREATE TABLE "HALTESTELLENBEREICH"
                            (   "NR" integer,
                                "HSTNR" integer REFERENCES "HALTESTELLE",
                                "CODE" varchar(255),
                                "NAME" varchar(255),
                                "KNOTNR" integer,
                                "TYPNR" integer,
                                "XKOORD" float NOT NULL,
                                "YKOORD" float NOT NULL,
                                PRIMARY KEY ("NR")
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "HALTEPUNKT" CASCADE')
        cursor.execute('''CREATE TABLE "HALTEPUNKT"
                            (   "NR" integer,
                                "HSTBERNR" integer REFERENCES "HALTESTELLENBEREICH",
                                "CODE" varchar(255),
                                "NAME" varchar(255),
                                "TYPNR" integer,
                                "VSYSSET" varchar(255),
                                "DEPOTFZGKOMBMENGE" varchar(255),
                                "GERICHTET" integer,
                                "KNOTNR" integer,
                                "VONKNOTNR" integer,
                                "STRNR" integer,
                                "RELPOS" float,
                                PRIMARY KEY ("NR")
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "KNOTEN" CASCADE')
        cursor.execute('''CREATE TABLE "KNOTEN"
                            (   "NR" integer,
                                "XKOORD" float NOT NULL,
                                "YKOORD" float NOT NULL,
                                PRIMARY KEY ("NR")
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "RICHTUNG" CASCADE')
        cursor.execute('''CREATE TABLE "RICHTUNG"
                            (   "NR" integer,
                                "CODE" varchar(255),
                                "NAME" varchar(255),
                                PRIMARY KEY ("CODE")
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "VSYS" CASCADE')
        cursor.execute('''CREATE TABLE "VSYS"
                            (   "CODE" varchar(255),
                                "NAME" varchar(255),
                                "TYP" varchar(255),
                                "PKWE" integer,
                                PRIMARY KEY ("CODE")
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "LINIE" CASCADE')
        cursor.execute('''CREATE TABLE "LINIE"
                            (   "NAME" varchar(255),
                                "VSYSCODE" varchar(255) REFERENCES "VSYS",
                                "TARIFSYSTEMMENGE" varchar(255),
                                "BETREIBERNR" integer REFERENCES "BETREIBER",
                                PRIMARY KEY ("NAME")
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "LINIENROUTE" CASCADE')
        cursor.execute('''CREATE TABLE "LINIENROUTE"
                            (   "LINNAME" varchar(255) REFERENCES "LINIE",
                                "NAME" varchar(255),
                                "RICHTUNGCODE" varchar(255),
                                "ISTRINGLINIE" integer,
                                PRIMARY KEY ("NAME")
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "LINIENROUTENELEMENT" CASCADE')
        cursor.execute('''CREATE TABLE "LINIENROUTENELEMENT"
                            (   "LINNAME" varchar(255),
                                "LINROUTENAME" varchar(255) REFERENCES "LINIENROUTE",
                                "RICHTUNGCODE" varchar(255),
                                "INDEX" integer,
                                "ISTROUTENPUNKT" integer,
                                "KNOTNR" integer,
                                "HPUNKTNR" integer
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "STRECKE" CASCADE')
        cursor.execute('''CREATE TABLE "STRECKE"
                            (   "NR" integer,
                                "VONKNOTNR" integer,
                                "NACHKNOTNR" integer,
                                "NAME" varchar(255),
                                "TYPNR" integer,
                                "VSYSSET" varchar(255)
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "STRECKENPOLY" CASCADE')
        cursor.execute('''CREATE TABLE "STRECKENPOLY"
                            (   "VONKNOTNR" integer,
                                "NACHKNOTNR" integer,
                                "INDEX" integer,
                                "XKOORD" float NOT NULL,
                                "YKOORD" float NOT NULL
                            )''')

        cursor.execute('DROP TABLE IF EXISTS "VERSION" CASCADE')
        cursor.execute('''CREATE TABLE "VERSION"
                            (   "VERSNR" float,
                                "FILETYPE" varchar(255),
                                "LANGUAGE" varchar(255)
                            )''')


        cursor.execute('INSERT INTO "VERSION" VALUES (%s, %s, %s)', ( 8.1, 'Net', 'DEU' ))

        cursor.execute('INSERT INTO "RICHTUNG" VALUES (%s, %s, %s)', ( 1, '>', ''))
        cursor.execute('INSERT INTO "RICHTUNG" VALUES (%s, %s, %s)', ( 2, '<', ''))

        cursor.execute( """CREATE OR REPLACE RULE ignore_duplicate_linien
                           AS ON INSERT TO "LINIE"
                           WHERE "NAME" IN ( SELECT "NAME" FROM "LINIE")
                           DO INSTEAD NOTHING""" )


        cursor.close()
        connection.commit()


    def _truncateDbTables(self):
        """ Leert Tabellen, ohne sie zu loeschen """

        connection = psycopg2.connect(self.db_connect_string)
        cursor = connection.cursor()

        TableNames = ["BETREIBER",
                      "FAHRZEITPROFIL",
                      "FAHRZEITPROFILELEMENT",
                      "FAHRPLANFAHRT",
                      "HALTESTELLE",
                      "HALTESTELLENBEREICH",
                      "HALTEPUNKT",
                      "KNOTEN",
                      "RICHTUNG",
                      "VSYS",
                      "LINIE",
                      "LINIENROUTE",
                      "LINIENROUTENELEMENT",
                      "STRECKE",
                      "STRECKENPOLY",
                      "VERSION"
                      ]
        for TableName in TableNames:
            cursor.execute('TRUNCATE "%s" CASCADE' %TableName)

        cursor.close()
        connection.commit()



    def _addPrimaryKey(self):
        """
        Adds Primary Keys to all Tables
        """
        connection = psycopg2.connect(self.db_connect_string)
        cursor = connection.cursor()

        cursor.execute('''ALTER TABLE "FAHRZEITPROFILELEMENT"
                          ADD PRIMARY KEY (
                                           "LINNAME",
                                           "LINROUTENAME",
                                           "RICHTUNGCODE",
                                           "NAME",
                                           "INDEX")
                      )''')

        cursor.execute('''ALTER TABLE "FAHRZEITPROFIL"
                          ADD PRIMARY KEY (
                                           "LINNAME",
                                           "LINROUTENAME",
                                           "RICHTUNGCODE",
                                           "NAME")
                      )''')

        cursor.execute('''ALTER TABLE "FAHRPLANFAHRT"
                          ADD PRIMARY KEY ("NR")
                      )''')

        cursor.execute('''ALTER TABLE "HALTEPUNKT"
                          ADD PRIMARY KEY ("NR")
                      )''')

        cursor.execute('''ALTER TABLE "HALTESTELLE"
                          ADD PRIMARY KEY ("NR")
                      )''')

        cursor.execute('''ALTER TABLE "HALTESTELLENBEREICH"
                          ADD PRIMARY KEY ("NR")
                      )''')

        cursor.execute('''ALTER TABLE "KNOTEN"
                          ADD PRIMARY KEY ("NR")
                      )''')

        cursor.execute('''ALTER TABLE "LINIE"
                          ADD PRIMARY KEY ("NAME")
                      )''')

        cursor.execute('''ALTER TABLE "LINIENROUTE"
                          ADD PRIMARY KEY (
                                           "LINNAME",
                                           "NAME",
                                           "RICHTUNGCODE")
                      )''')

        cursor.execute('''ALTER TABLE "LINIENROUTENELEMENT"
                          ADD PRIMARY KEY (
                                           "LINNAME",
                                           "LINROUTENAME",
                                           "RICHTUNGCODE",
                                           "INDEX")
                      )''')

        cursor.execute('''ALTER TABLE "STRECKE"
                          ADD PRIMARY KEY (
                                           "NR",
                                           "VONKNOTNR",
                                           "NACHKNOTNR")
                      )''')

        cursor.execute('''ALTER TABLE "STRECKENPOLY"
                          ADD PRIMARY KEY (
                                           "VONKNOTNR",
                                           "NACHKNOTNR",
                                           "INDEX")
                      )''')

        cursor.execute('''ALTER TABLE "VERSION"
                          ADD PRIMARY KEY ("VERSNR")
                      )''')

        cursor.close()
        connection.commit()

