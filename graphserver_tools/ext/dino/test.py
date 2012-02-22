#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      LISSABON
#
# Created:     17.01.2012
# Copyright:   (c) LISSABON 2012
# Licence:     <your licence>
#-------------------------------------------------------------------------------
#!/usr/bin/env python

def main():
    pass

if __name__ == '__main__':
    main()


##read_dino.getSession(create_tables=True)
##read_dino.read_table('set_version')
##read_dino.read_table('rec_stop')
##read_dino.read_table('rec_stop_area')
##
##read_dino.read_table('rec_trip')

from graphserver_tools.ext.dino import *
from graphserver_tools.ext.dino.models import *
read_dino.getSession(create_tables=True)
tables = [tn[5:] for tn in Base.metadata.tables.keys()]
for tn in tables:
    print tn
    read_dino.read_table(tn)
