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
    tables = ['set_version',
              'rec_stop',
              'rec_stop_area',
              'rec_stopping_points',
              'set_day_attribute',
              'set_day_type',
              'day_type_2_day_attribute',
              'calendar_of_the_company',
              'hinw_str',
              'rec_lin_ber',
              'lid_course',
              'lid_travel_time_type',
              'rec_trip',
             'notice',
             'set_depot',
             'branch',
             'set_vehicle_type',
             'vehicle_destination_text',
             'service_interdiction',
             'rec_round_trip',
             'trip_stop_time',
             'set_trip_purpose',
             'trip_vdt',
             'rec_connection',
             'rec_footpath',
             'service_restriction']
    for tn in tables:
        print tn
##        read_dino.read_table(tn, folder=r'D:\GIT\gs\graphserver_tools\graphserver_tools\msp\Eingangsdaten\01 Dino\Fahrplan DVG')
        read_dino.read_table(tn, folder=r'D:\GIT\gs\graphserver_tools\graphserver_tools\msp\Eingangsdaten\01 Dino\Fahrplan VMS')

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

