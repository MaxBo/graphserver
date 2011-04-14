from setuptools import setup

setup(  name='graphserver_tools',
        version='0.30',
        author='Tobias Ottenweller',
        packages=['graphserver_tools', 'graphserver_tools.utils', 'graphserver_tools.ext' ],
        install_requires=['graphserver>=1.0.0','pyproj>=1.8.8', 'psycopg2' ],

        entry_points={
            'console_scripts': [
                'gst_process = graphserver_tools.process:main',
                'gst_netToGtfs = graphserver_tools.ext.netToGtf:main',
                'gst_hafasToGtfs = graphserver_tools.ext.hafasToGtf:main',
            ],
        }
     )