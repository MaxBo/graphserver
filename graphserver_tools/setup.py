from setuptools import setup

setup(  name='graphserver_tools',
        version='0.52',
        author='Tobias Ottenweller, Max Bohnet, Christine Walther, Achim Tack',
        packages=['graphserver_tools', 'graphserver_tools.utils', 'graphserver_tools.ext', 'graphserver_tools.ext.ivu', 'graphserver_tools.ext.ivu.models' ],
        install_requires=['graphserver>=1.0.0','pyproj>=1.8.8', 'psycopg2', 'termcolor', 'transitfeed', 'sqlalchemy', 'bitstring' ],

        entry_points={
            'console_scripts': [
                'gst_process = graphserver_tools.process:main',
                'gst_netToGtfs = graphserver_tools.ext.netToGtf:main',
                'gst_hafasToGtfs = graphserver_tools.ext.hafasToGtf:main',
                'gst_cropOSM = graphserver_tools.ext.cropOSM:main',
                'gst_gtfsToVisum = graphserver_tools.ext.gtfToVisum:main',
                'gst_ivuToVisum = graphserver_tools.ext.ivuToVisum:main',
            ],
        }
     )
