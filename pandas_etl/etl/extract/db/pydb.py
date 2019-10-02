"""
Database util class to make database connection and read data from table
"""

import pymysql
import psycopg2
import pymssql
import cx_Oracle as oracle
import pandas as pd 
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
	conn_dict = {
		'mysql' : pymysql.connect(host=host, port=port, user=username, passwd=password, db=db_name),
		'mariadb' : pymysql.connect(host=host, port=port, user=username, passwd=password, db=db_name),
		'rds' : pymysql.connect(host=host, port=port, user=username, passwd=password, db=db_name),
		'postgresql' : psycopg2.connect(host=host, port=port, user=username, password=password, dbname=db_name),
		'redshift' : psycopg2.connect(host=host, port=port, user=username, password=password, dbname=db_name),
		'mssql' : pymssql.connect(host=host, port=port, user=username, password=password, database=db_name),
		'oracle' : oracle.connect(username+'/'+password+'@'+host+':'+str(port)+'/'+db_name),
		'hive' : hive.connect(host=host, port=port, username=username, password = password, database=db_name)
	}

	# if db_type.lower() in ['mysql','mariadb','rds'] :
	# 	conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=db_name)
	# elif db_type.lower() in ['postgresql','redshift'] :
	# 	conn = psycopg2.connect(host=host, port=port, user=username, password=password, dbname=db_name)
	# elif db_type.lower() in ['mssql'] :
	# 	conn = pymssql.connect(host=host, port=port, user=username, password=password, database=db_name)
	# elif db_type.lower() in ['oracle'] :
	# 	conn = oracle.connect(username+'/'+password+'@'+host+':'+str(port)+'/'+db_name)
	# elif db_type.lower() in ['hive'] :
	# 	conn = hive.connect(host=host, port=port, username=username, password = password, database=db_name)
	
	conn = conn_dict.get(db_type.lower(),None)

	if conn is None:
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
	try:
		df = pd.read_sql(query,con=connection)
	except Exception as e:
		raise e

	return df

def get_cassandra_connection(hosts):
	"""
	Function to get Cassandra Connection

	Parameter
	---------

	hosts : List[str] - List of IPs


	Return
	------

	session : Cassandra Session

	"""
	if not isinstance(hosts,list):
		raise Exception('hosts are expected in List')
	try:
		cluster = Cluster(hosts)
		cassandra_session = cluster.connect()
	except Exception as e:
		raise Exception("Error in cassandra database connection")
	print("------------ Cassandra connection created ---------------")
	return cassandra_session
