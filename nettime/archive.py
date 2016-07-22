import numpy as np
import pandas as pd
import email, email.parser
import os, datetime, json, gzip, re
from random import randint
import query

def format_from(from_str):
	from_addr = email.utils.parseaddr(from_str)[1]
	if '{AT}' not in from_addr:
		tok = from_str.split()
		try:
			at = tok.index('{AT}')
			from_addr = ''.join(tok[at-1:at+2])
			if from_addr.startswith('<') or from_addr.endswith('>'):
				from_addr = from_addr.strip('<').strip('>')
		except ValueError:
			return None
	return from_addr.lower()

def format_date(date_str):
	try:
		date_tz = email.utils.parsedate_tz(date_str)
		time_tz = email.utils.mktime_tz(date_tz) #utc timestamp
	except TypeError:
		print "Format Date TypeError"
		print "  > " + date_str
		return None
	except ValueError:
		print "Format Date ValueError"
		print "  > " + date_str
		return None

	dt = datetime.datetime.fromtimestamp(time_tz)

	try:
		pdt = pd.to_datetime(dt)
		return pdt
	except pd.tslib.OutOfBoundsDatetime:
		print 'time out of bound'
		print dt
		return None

def message_to_tuple_record(msg, records, references=None):

	# check date first?
	date_time = format_date(msg['date'])
	if not date_time:
		return

	# filter date?
	nettime_min_date = pd.to_datetime('01/10/1995', format='%d/%m/%Y')
	nettime_max_date = pd.to_datetime(datetime.datetime.now())
	if date_time < nettime_min_date or date_time > nettime_max_date:
		return None

	# check / filter from email address second?
	from_addr = format_from(msg['from'])
	if not from_addr:
		return

	records.append((msg['message-id'],
						from_addr,
						msg['author_name'],
						msg['subject'],
						date_time,
						msg['url'],
						len(msg['content']),
						0 if not msg.has_key('follow-up') else len(msg['follow-up']),
						references))

	if msg.has_key('follow-up'):
		for f in msg['follow-up']:
			message_to_tuple_record(f, records, references=msg['message-id'])

	return 

def json_data_to_pd_dataframe(json_data):

	records = []
	for d in json_data:
		for dd in d['threads']:
			message_to_tuple_record(dd, records)

	df = pd.DataFrame.from_records(records,
						index='date',
						columns=['message-id',
									'from',
									'author',
									'subject',
									'date',
									'url',
									'content-length',
									'nbr-references',
									'references'])

	df.index.name = 'date'

	return df

def load_from_file(filename, archive_dir, json_data=None):

	if not filename.endswith('.json.gz'):
		file_path = os.path.join(archive_dir, filename + '.json.gz')
	else:
		file_path = os.path.join(archive_dir, filename)

	if os.path.isfile(file_path):
		with gzip.open(file_path, 'r') as fp:
			json_data = json.load(fp)
			return json_data_to_pd_dataframe(json_data['threads'])
	else:
		#list of all "filename[...].json.gz" in archive_dir
		files = sorted([f for f in os.listdir(archive_dir) if os.path.isfile(os.path.join(archive_dir, f)) and f.startswith(filename) and f.endswith('.json.gz')])
		if files:
			filename = files[-1] # take the most recent (listed alpha-chronological)
			file_path = os.path.join(archive_dir, filename)
			if os.path.isfile(file_path):
				with gzip.open(file_path, 'r') as fp:
					json_data = json.load(fp)
					return json_data_to_pd_dataframe(json_data['threads'])
		else:
			#list of all json files in archive_dir/filename
			dir_path = os.path.join(archive_dir, filename)
			if not os.path.isdir(dir_path):
				return None

			files = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f)) and f.endswith('.json')]
			if not files:
				return None

			# load all json files
			threads = []
			for file_path in files:
				with open(file_path, 'r') as fp:
					json_data = json.load(fp)
					threads.append(json_data)

			return json_data_to_pd_dataframe(threads)
				

class Archive:

	data = None				# "raw" json data
	dataframe = None 		# main pd dataframe

	def __init__(self, data="nettime-l", archive_dir="archives"):

		if isinstance(data, pd.core.frame.DataFrame):
			self.dataframe = data.copy()

		if isinstance(data, str):
			self.dataframe = load_from_file(data, archive_dir, self.data)

	def query(self):
		q = query.Query(self)
		return q

