
from setuptools import setup, find_packages

VERSION = '0.0.8'
DESCRIPTION = 'IceDB'
LONG_DESCRIPTION = 'IceDB in-process serverless OLAP powered by DuckDB'

# Setting up
setup(
    # the name must match the folder name 'verysimplemodule'
    name="icedb",
    version=VERSION,
    author="Dan Goodman",
    author_email="dan@danthegoodman.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[
        "boto3==1.26.151",
        "botocore==1.29.151",
        "duckdb==0.8.1.dev416",
        "pandas==2.0.2",
        "psycopg2==2.9.6"
    ],
    keywords=['olap', 'icedb', 'data lake'],
    classifiers= []
)
