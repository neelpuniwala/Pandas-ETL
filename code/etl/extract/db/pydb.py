"""
Database util class to make database connection and read data from table
"""

import pymysql
import psycopg2
import pymssql
import cx_Oracle as oracle
from pyhive import hive
import pandas as pd 

class db_utils:
	"""
	Class For Database Connection and Table Data Reading
	"""

	def __init__(self):
		"""
		Contructor
		"""


	def connect_database(self,db_type,host,username,password,port,db_name):
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
		try:
			if db_type.tolower() in ['mysql','mariadb','rds'] :
				conn = pymysql.connect(hosthost, port=port, user=username, passwd=password, db=db_name)
			elif db_type.tolower() in ['postgresql','redshift'] :
				conn = psycopg2.connect(host=host, port=port, user=username, password=password, dbname=db_name)
			elif db_type.tolower() in ['mssql'] :
				conn = pymssql.connect(host=host, port=port, user=username, password=password, database=db_name)
			elif db_type.tolower() in ['oracle'] :
				conn = oracle.connect(username+'/'+password+'@'+host+':'+str(port)+'/'+db_name)
			elif db_type.tolower() in ['hive'] :
				conn = hive.connect(host=host, port=port, username=username, password = password, database=db_name)
			else :
				raise Exception(db_type+" Database Type is not Supported")
		except Exception as e:
			raise e

		return conn 


	def read_db_table(self,connection,query):
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
