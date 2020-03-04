"""
Database util class to make database connection and read data from table
"""
import pandas as pd 

import pymysql
import psycopg2
import pymssql
import cx_Oracle as oracle
import cassandra
from pyhive import hive
from cassandra.cluster import Cluster


def connect_database(db_type,host,username,password,port,db_name):
    """
    Fuction to get database connection

    Parameters
    ----------

    db_type : str - Database Type Supported MySQL,MariaDB,RDS,PostgreSQL,Redshift,MsSQL,Oracle,Hive
    host : str - Database host IP
    username : str - Username for Database host
    password : str - Password for Username
    port : int - Port on which Database Server running 
    db_name : str - Nmae of the Schema

    Returns
    -------

    conn : Connection Object - Database Connection Object

    Raises
    ------

    Exception - db_type pass which is not supported

    """
    if db_type.lower() in ['mysql','mariadb','rds'] :
        conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=db_name)
    elif db_type.lower() in ['postgresql','redshift'] :
        conn = psycopg2.connect(host=host, port=port, user=username, password=password, dbname=db_name)
    elif db_type.lower() in ['mssql'] :
        conn = pymssql.connect(host=host, port=port, user=username, password=password, database=db_name)
    elif db_type.lower() in ['oracle'] :
        conn = oracle.connect(username+'/'+password+'@'+host+':'+str(port)+'/'+db_name)
    elif db_type.lower() in ['hive'] :
        conn = hive.connect(host=host, port=port, username=username, password = password, database=db_name)
    else:
        raise Exception(db_type+" Database Type is not Supported")

    return conn 


def read_db_table(connection,query):
    """
    Fuction read data from table and create Pandas DataFrame

    Parameter
    ---------

    connection : Database Connection Object 
    query : str - Query to Perform on Database

    Return
    ------

    df : pd.DataFrame - Pandas Dataframe created using reading Data

    """
    if(isinstance(connection,cassandra.cluster.Session)):
        df = __create_dataframe_from_cassandra(query,con=connection)
    else:
        df = pd.read_sql(query,con=connection)
    return df

def get_cassandra_connection(hosts,keyspace):
    """
    Function to get Cassandra Connection

    Parameter
    ---------

    hosts : List[String] - List of IPs
    keyspace : String - Name of Keyspace

    Return
    ------

    session : Cassandra Session

    """
    if not isinstance(hosts,list):
        raise Exception('List is expected.')

    cluster = Cluster(hosts)
    cassandra_session = cluster.connect()
    cassandra_session.set_keyspace(keyspace)

    return cassandra_session

def __create_dataframe_from_cassandra(query,con):
    """
    Function to query into Cassandra and Create Pandas DataFrame
    
    Parameter
    ---------
    
    query : String - Cassandra Query
    con : cassandra connection object
    
    Return
    ------
    
    df : pd.DataFrame - DataFrame created using the cassandra query output
    
    """
    all_records = list(con.execute(query))
    df = pd.DataFrame(all_records)
    return df