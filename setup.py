from setuptools import setup

setup(  name='graphserver_tools',
        version='0.21',
        author='Tobias Ottenweller',
        packages=['graphserver_tools', ],
        install_requires=['graphserver>=1.0.0','pyproj>=1.8.8',],

        entry_points={
            'console_scripts': [
                'gst_process = graphserver_tools.process:main',
                'gst_netToGtfs = graphserver_tools.netToGtf:main',
                'gst_hafasToGtfs = graphserver_tools.hafasToGtf:main',
            ],
        }
     )