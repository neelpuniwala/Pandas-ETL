"""
File util class to read data from File and write data to file
"""

import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class file_utils:
	"""
	Class contains functions to read different types of files and create dataframe from data
	"""

	def __init__(self):
		"""
		Contructor
		"""

	def read_csv_file(self,filepath,seperator=',',date_columns=[],column_name=[]):
		"""
		Function to read CSV,TSV flat structured file

		Parameter : filepath : str - Input File Path
		Parameter : seperator : str - Field Seperator (Default = ,)
		Parameter : date_columns : list - List of datetime columns
		Parameter : column_name : list - List of column names

		Return : df : pd.DataFrame - Pandas Dataframe created using reading Data

		"""
		try:
			if column_name:
				df = pd.read_csv(filepath,escapechar="\\",sep=seperator,names=column_name,parse_dates=date_columns)
			else:
				df = pd.read_csv(filepath,escapechar="\\",sep=seperator,parse_dates=date_columns)

		except Exception as e:
			raise

		return df


	def read_excel_file(self,filepath,sheet_name=0,date_columns=[],column_name=[]):
		"""
		Function to read xlsx file

		Parameter : filepath : str - Input File Path
		Parameter : sheet_name : str,int or None - str for specific sheet name, int for index no. of sheet and None for all the sheet
		Parameter : date_columns : list - List of datetime columns
		Parameter : column_name : list - List of column names

		Return : df : pd.DataFrame - Pandas Dataframe created using reading Data

		"""
		try:
			if column_name:
				df = pd.read_excel(filepath,sheet_name=sheet_name,names=column_name,parse_dates=date_columns)
			else:
				df = pd.read_excel(filepath,sheet_name=sheet_name,parse_dates=date_columns)

		except Exception as e:
			raise e

		return df

	def read_parquet_file(self,filepath):
		"""
		Function to read parquet file

		Parameter : filepath : str - Input File Path

		Return : df : pd.DataFrame - Pandas Dataframe created using reading Data

		"""
		try:
			df = pq.read_table(filepath)
			df = df.to_pandas()

		except Exception as e:
			raise e

		return df

	def read_json(self,input_str):
		"""
		Function to read json file or Stringyfy Json

		Note : Support for only Specific Json Format which are supported by Pandas

		Parameter : input_str : str - Input File Path or Stringyfy Json

		Return : df : pd.DataFrame - Pandas Dataframe created using reading Data

		"""
		try:
			if(os.path.isfile(input)):
				with open(input_str, "r") as json_file:
					input_json = json.load(json_file)
				df = pd.DataFrame(input_json)
			else:
				input_json = json.load(input_str)
				df = pd.DataFrame(input_json)

		except Exceptin as e:
			raise e

		return df

	def write_csv_file(self,df,filepath):
		"""
		Function to write csv file

		Parameter : df : pd.DataFrame - Input File Path or Stringyfy Json
		Parameter : filepath : str - Output File Path

		"""
		try:
			os.makedirs(os.path.dirname('/'.join(filepath.split('/')[:-1])), exist_ok=True)
			df.to_csv(filepath,encoding='utf-8',index=False)
		except Exception as e:
			raise e

	def write_excel_file(self,df,filepath):
		"""
		Function to write excel file

		Parameter : df : pd.DataFrame - Input File Path or Stringyfy Json
		Parameter : filepath : str - Output File Path

		"""
		try:
			os.makedirs(os.path.dirname('/'.join(filepath.split('/')[:-1])), exist_ok=True)
			df.to_excel(filepath)
		except Exception as e:
			raise e

	def write_parquet_file(self,df,filepath,partition_columns = [],compression='snappy'):
		"""
		Function to write parquet file

		Parameter : df : pd.DataFrame - Input File Path or Stringyfy Json
		Parameter : filepath : str - Output File Path
		Parameter : partition_columns : list - list of partition columns
		Parameter : compression : str - Compression Type Default - snappy (snappy,gzip,brotli,none)

		"""
		try:
			os.makedirs(os.path.dirname('/'.join(filepath.split('/')[:-1])), exist_ok=True)
			df_table = pa.Table.from_pandas(df,preserve_index = False)
			if partition_columns :
				pq.write_to_dataset(df_table,root_path=filepath,partition_cols=partition_columns,compression=compression)
			else:
				pq.write_to_dataset(df_table,root_path=filepath,compression=compression)

		except Exception as e:
			raise e

	def write_json_file(self,df,filepath,orient='records'):
		"""
		Function to Json parquet file

		Parameter : df : pd.DataFrame - Input File Path or Stringyfy Json
		Parameter : filepath : str - Output File Path
		Parameter : orient : str - Compression Type Default - records (records,dict,list,series,split,index)

		"""
		try:
			os.makedirs(os.path.dirname('/'.join(filepath.split('/')[:-1])), exist_ok=True)
			with open(filepath,'w') as json_file:
				json.dumps(df.to_dict(orient=orient))
	
		except Exception as e:
			raise e







