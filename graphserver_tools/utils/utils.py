#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Author Tobias Ottenweller
# 21.10.2010 - 03.12.2010
# Gertz Gutsche Rümenapp Gbr


import copy
import sqlite3
import math
from pyproj import Proj, transform

from graphserver.core import Street


def read_config(filename, defaults):
    try:
        config = copy.copy(defaults)

        f = open(filename)

        for line in f:
            if line[0] not in ( '#', '\n' ):
                stuff = line.split('=')
            
                if stuff[0] in config:
                    config[stuff[0]] = stuff[1][:-1]
    except:
        print('\tERROR: could not read configuration')
        return defaults

    return config        


def time_adder(time_string1, time_string2):
    """ Adds two times (format: HH:MM:SS) together and returns the result. May return hours greater then 23. """
    ( h1, m1, s1 ) = time_string1.split(':')
    ( h2, m2, s2 ) = time_string2.split(':')
    h = int(h1) + int(h2)
    m = int(m1) + int(m2)
    s = int(s1) + int(s2)
    while s > 59: s -= 60; m += 1
    while m > 59: m -= 60; h += 1
    
    return '%02d:%02d:%02d' % (h, m, s)

def coord_to_wgs84(from_proj, x, y, z='0'):
    ''' Transforms given coordinates to WGS84. from_proj needs to be a Proj object,
        x, y and z need to be strings. 
    '''
    wgs84 = Proj(proj='latlon', datum='WGS84')
            
    x = float(x.replace(',', '.'))
    y = float(y.replace(',', '.'))
    z = float(z.replace(',', '.'))
            
    return transform(from_proj, wgs84, x, y, z)

    
def eliminate_blank_lines(filename, eol_character):
    lines = []
    
    with open(filename, 'r') as file:
        for l in file:
            if not l == eol_character:
                lines.append(l)
    
    
    with open(filename, 'w') as file:  
        for l in lines:
            file.write(l)



def delete_bad_edges(osmdb_filename):
    conn = sqlite3.connect( osmdb_filename )
    cursor = conn.cursor()
    
    updated_nodes = cursor.execute('SELECT start_nd FROM edges WHERE start_nd=end_nd').fetchall()
    
    for n in updated_nodes:
        cursor.execute('UPDATE nodes SET endnode_refs = endnode_refs - 1 WHERE id=?',  n)
    
    cursor.execute('DELETE FROM edges WHERE start_nd = end_nd')
    
    conn.commit()



def distance(lat1, long1, lat2, long2):
    ''' original code from http://www.johndcook.com/python_longitude_latitude.html'''

    degrees_to_radians = math.pi/180.0
    
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians
    
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians
    
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )
    
    return arc * 6373000


def disable_bad_edges(graph): # does not save the changes
    for v in graph.vertices:
        for e in v.outgoing:
            if e.payload.__class__ == Street('a', 1).__class__:
                if e.payload.length == 0:
                    if e.to_v.label == e.from_v.label:
                        e.enabled = 0
    
    
    
    
    
    
    
    
