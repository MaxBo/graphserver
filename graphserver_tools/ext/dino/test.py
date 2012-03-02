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

def test_import():
    from graphserver_tools.ext.dino import *
    from graphserver_tools.ext.dino.models import *
    read_dino.getSession(create_tables=True)
    tables = [tn[5:] for tn in Base.metadata.tables.keys()]
    for tn in tables:
        print tn
        read_dino.read_table(tn, folder=r'D:\GIT\gs\graphserver_tools\graphserver_tools\msp\Eingangsdaten\01 Dino\Fahrplan DVG')

##test_import()

from graphserver_tools.ext.dinoToVisum import DinoToVisum
D = DinoToVisum()
D._getDirections()
D._processBetreiber()
D._processVsysset()
#D._processZwischenpunkte()
##D._processHstHstBereichHstPunkt()
D._processLinie()
D._processLinienroutenelement()
D._processFahrzeitprofil()
print D

