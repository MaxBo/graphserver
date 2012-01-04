#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller, Max Bohnet
# 21.10.2011
# Gertz Gutsche Rümenapp Gbr

import psycopg2
import threading

class VisumPuTTables(object):


    def _createDbTables(self, drop=False):
        """ Creates all necessary database tables. Won't overwrite tables unless 'drop' is
            set to True.
        """


        connection = psycopg2.connect(self.db_connect_string)
        cursor = connection.cursor()

        if drop:
            cursor.execute('''DROP TABLE IF EXISTS  "BETREIBER",
                                                    "FAHRZEITPROFIL",
                                                    "FAHRZEITPROFILELEMENT",
                                                    "FAHRPLANFAHRT",
                                                    "FAHRPLANFAHRTABSCHNITT",
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
                              CASCADE''')


        cursor.execute("select tablename from pg_tables where schemaname='public'" )
        tables = [t[0] for t in cursor.fetchall()]


        if 'BETREIBER' not in tables:
            cursor.execute('''CREATE TABLE "BETREIBER"
                                    (   "NR" integer,
                                        "NAME" varchar(255),
                                        "KOSTENSATZ1" float,
                                        "KOSTENSATZ2" float,
                                        "KOSTENSATZ3" float,
                                        PRIMARY KEY ("NR")
                                    )''')


        if 'FAHRZEITPROFIL' not in tables:
            cursor.execute('''CREATE TABLE "FAHRZEITPROFIL"
                                    (   "LINNAME" varchar(255),
                                        "LINROUTENAME" varchar(255),
                                        "RICHTUNGCODE" varchar(255),
                                        "NAME" varchar(255),
                                        PRIMARY KEY (   "LINNAME",
                                                        "LINROUTENAME",
                                                        "RICHTUNGCODE",
                                                        "NAME"
                                                    )
                                    )''')


        if 'FAHRZEITPROFILELEMENT' not in tables:
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
                                        "ABFAHRT" timestamp,
                                        PRIMARY KEY (   "LINNAME",
                                                        "LINROUTENAME",
                                                        "RICHTUNGCODE",
                                                        "INDEX",
                                                        "FZPROFILNAME"
                                                    )
                                    )''')


        if 'FAHRPLANFAHRT' not in tables:
            cursor.execute('''CREATE TABLE "FAHRPLANFAHRT"
                                (   "NR" integer,
                                    "NAME" varchar(255),
                                    "ABFAHRT" timestamp,
                                    "LINNAME" varchar(255),
                                    "LINROUTENAME" varchar(255),
                                    "RICHTUNGCODE" varchar(255),
                                    "FZPROFILNAME" varchar(255),
                                    "VONFZPELEMINDEX" integer,
                                    "NACHFZPELEMINDEX" integer,
                                    PRIMARY KEY ("NR")
                                )''')

        if 'FAHRPLANFAHRTABSCHNITT' not in tables:
            cursor.execute('''CREATE TABLE "FAHRPLANFAHRTABSCHNITT"
                                (   "NR" integer,
                                    "FPLFAHRTNR" integer,
                                    "VONFZPELEMINDEX" integer,
                                    "NACHFZPELEMINDEX" integer,
                                    PRIMARY KEY ("FPLFAHRTNR", "NR")
                                )''')


        if 'HALTESTELLE' not in tables:
            cursor.execute('''CREATE TABLE "HALTESTELLE"
                                (   "NR" integer,
                                    "CODE" varchar(255),
                                    "NAME" varchar(255),
                                    "TYPNR" integer,
                                    "XKOORD" float NOT NULL,
                                    "YKOORD" float NOT NULL,
                                    PRIMARY KEY ("NR")
                                )''')


        if 'HALTESTELLENBEREICH' not in tables:
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


        if 'HALTEPUNKT' not in tables:
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


        if 'KNOTEN' not in tables:
            cursor.execute('''CREATE TABLE "KNOTEN"
                                (   "NR" integer,
                                    "XKOORD" float NOT NULL,
                                    "YKOORD" float NOT NULL,
                                    PRIMARY KEY ("NR")
                                )''')


        if 'RICHTUNG' not in tables:
            cursor.execute('''CREATE TABLE "RICHTUNG"
                                (   "NR" integer,
                                    "CODE" varchar(255),
                                    "NAME" varchar(255),
                                    PRIMARY KEY ("CODE")
                                )''')


        if 'VSYS' not in tables:
            cursor.execute('''CREATE TABLE "VSYS"
                                (   "CODE" varchar(255),
                                    "NAME" varchar(255),
                                    "TYP" varchar(255),
                                    "PKWE" integer,
                                    PRIMARY KEY ("CODE")
                                )''')


        if 'LINIE' not in tables:
            cursor.execute('''CREATE TABLE "LINIE"
                                (   "NAME" varchar(255),
                                    "VSYSCODE" varchar(255) REFERENCES "VSYS",
                                    "TARIFSYSTEMMENGE" varchar(255),
                                    "BETREIBERNR" integer REFERENCES "BETREIBER",
                                    PRIMARY KEY ("NAME")
                                )''')


        if 'LINIENROUTE' not in tables:
            cursor.execute('''CREATE TABLE "LINIENROUTE"
                                (   "LINNAME" varchar(255) REFERENCES "LINIE",
                                    "NAME" varchar(255),
                                    "RICHTUNGCODE" varchar(255),
                                    "ISTRINGLINIE" integer,
                                    PRIMARY KEY (   "LINNAME",
                                                    "NAME",
                                                    "RICHTUNGCODE"
                                                )
                                )''')


        if 'LINIENROUTENELEMENT' not in tables:
            cursor.execute('''CREATE TABLE "LINIENROUTENELEMENT"
                                (   "LINNAME" varchar(255),
                                    "LINROUTENAME" varchar(255),
                                    "RICHTUNGCODE" varchar(255),
                                    "INDEX" integer,
                                    "ISTROUTENPUNKT" integer,
                                    "KNOTNR" integer,
                                    "HPUNKTNR" integer,
                                    PRIMARY KEY (   "LINNAME",
                                                    "LINROUTENAME",
                                                    "RICHTUNGCODE",
                                                    "INDEX"
                                                ),
                                    FOREIGN KEY  (  "LINNAME",
                                                    "LINROUTENAME",
                                                    "RICHTUNGCODE"
                                                 ) REFERENCES "LINIENROUTE"
                                )''')


        if 'STRECKE' not in tables:
            cursor.execute('''CREATE TABLE "STRECKE"
                                (   "NR" integer,
                                    "VONKNOTNR" integer,
                                    "NACHKNOTNR" integer,
                                    "NAME" varchar(255),
                                    "TYPNR" integer,
                                    "VSYSSET" varchar(255),
                                    PRIMARY KEY (   "NR",
                                                    "VONKNOTNR",
                                                    "NACHKNOTNR"
                                                )
                                )''')


        if 'STRECKENPOLY' not in tables:
            cursor.execute('''CREATE TABLE "STRECKENPOLY"
                                (   "VONKNOTNR" integer,
                                    "NACHKNOTNR" integer,
                                    "INDEX" integer,
                                    "XKOORD" float NOT NULL,
                                    "YKOORD" float NOT NULL,
                                    PRIMARY KEY (   "VONKNOTNR",
                                                    "NACHKNOTNR",
                                                    "INDEX"
                                                )
                                )''')


        if 'VERSION' not in tables:
            cursor.execute('''CREATE TABLE "VERSION"
                                (   "VERSNR" float,
                                    "FILETYPE" varchar(255),
                                    "LANGUAGE" varchar(255),
                                    PRIMARY KEY ("VERSNR")
                                )''')


        if 'UEBERGANGSGEHZEITHSTBER' not in tables:
            cursor.execute('''CREATE TABLE "UEBERGANGSGEHZEITHSTBER"
                                (   "VONHSTBERNR" integer,
                                    "NACHHSTBERNR" integer,
                                    "VSYSCODE" varchar(255),
                                    "ZEIT" integer,
                                    PRIMARY KEY (   "VONHSTBERNR",
                                                    "NACHHSTBERNR",
                                                    "VSYSCODE"
                                                )
                                )''')

        cursor.close()
        connection.commit()


    def _truncateDbTables(self):
        """ empty all tables without droping them """

        connection = psycopg2.connect(self.db_connect_string)
        cursor = connection.cursor()


        cursor.execute('''TRUNCATE "BETREIBER",
                                   "FAHRZEITPROFIL",
                                   "FAHRZEITPROFILELEMENT",
                                   "FAHRPLANFAHRT",
                                   "FAHRPLANFAHRTABSCHNITT",
                                   "HALTESTELLE",
                                   "HALTESTELLENBEREICH",
                                   "HALTEPUNKT",
                                   "KNOTEN",
                                   "VSYS",
                                   "LINIE",
                                   "LINIENROUTE",
                                   "LINIENROUTENELEMENT",
                                   "STRECKE",
                                   "STRECKENPOLY",
                                   "VERSION",
                                   "RICHTUNG",
                                   "UEBERGANGSGEHZEITHSTBER"
                          CASCADE''')

        cursor.execute('INSERT INTO "VERSION" VALUES (%s, %s, %s)', ( 8.1, 'Net', 'DEU' ))

        cursor.execute('INSERT INTO "RICHTUNG" VALUES (%s, %s, %s)', ( 1, '>', ''))
        cursor.execute('INSERT INTO "RICHTUNG" VALUES (%s, %s, %s)', ( 2, '<', ''))

        cursor.close()
        connection.commit()
