from setuptools import setup
setup(
    name='db_utils',
    version='1.0',
    author='Courtney Wade',
    description='Utilities for querying and loading data into an Oracle database. Queries return to pandas dataframes. Faster than SQLAlchemy.',
    long_description='Utilities for querying and loading data into an Oracle database. Eventually planning to add support for other database types',
    url='https://github.com/BenjaminFranline',
    keywords='pandas, oracle, query',
    python_requires='>=3.7, <4',
    install_requires=[
        'oracledb>=1.2',
        'pandas>=1.0',
        'PyYAML>=6.0'
    ]
)