import numpy as np
import pandas as pd
import email, email.parser
import os, datetime, json, gzip, re
from random import randint

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

def load_from_file(filename, archive_dir):

	json_data = None
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

	activity = None			# (very) sparse dataframe (index=date(month), columns=from, values=activity(month))
	content_length = None	# (very) sparse dataframe (index=date(month), columns=from, values=content-length(month in bytes))

	threads = None

	def __init__(self, data="nettime-l", archive_dir="archives"):

		if isinstance(data, pd.core.frame.DataFrame):
			self.dataframe = data.copy()

		if isinstance(data, str):
			self.dataframe = load_from_file(data, archive_dir)

	'''
	activity
	'''			

	def _activity(self):

		if self.activity is None:
			from_index = self.dataframe.reindex(columns=['from'])
			self.activity = from_index.groupby([pd.TimeGrouper(freq='M'), 'from']).size().unstack('from').fillna(0)

		return self.activity

	def activity_from(self, email_address, resolution='M'):

		eaddr = email_address.replace('@', '{at}').lower()

		self._activity()
		try:
			if resolution.lower() == 'm':
				return self.activity[eaddr]
			elif resolution.lower() == 'y':
				y = self.activity[eaddr].resample('AS').sum()
				y.index = y.index.year
				return y
			else:
				return None
		except KeyError:
			return None

	def activity_overall(self, resolution='M'):

		self._activity()
		try:
			sum_activity_month = self.activity.sum(axis=1)
			if resolution.lower() == 'm':
				return sum_activity_month
			elif resolution.lower() == 'y':
				y = sum_activity_month.resample('AS').sum()
				y.index = y.index.year
				return y
			else:
				return None
		except:
			return None

	def activity_from_ranking(self, resolution='M', rank=5, filter_nettime=True):
		# finish this -- re resolution AND filtering
		self._activity()
		afr = self.activity.sum(axis=0).order(ascending=False)
		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			afr = afr[afr.index.str.contains(p)]
		return afr[:rank]

	def plot_activity_from_ranking(self, resolution='y', rank=5, figsize=(8, 7)):

		activity_rank = self.activity_from_ranking(rank=rank).keys()
		series = []
		for k in activity_rank:
			series.append(self.activity_from(k, resolution))
			
		df = pd.concat(series, axis=1)
		
		colors = np.random.rand(len(df),3)

		if figsize:
			df.plot(colors=colors, figsize=figsize)
		else:
			df.plot(colors=colors)

	'''
	content lenght
	'''

	def _content_length(self):

		if self.content_length is None:
			from_content_index = self.dataframe.reindex(columns=['from', 'content-length'])
			self.content_length = from_content_index.groupby([pd.TimeGrouper(freq='M'), 'from']).sum()
			self.content_length = self.content_length.reset_index().pivot(columns='from', index='date', values='content-length').fillna(0)

		return self.content_length

	def content_length_from(self, email_address, resolution='M'):

		eaddr = email_address.replace('@', '{at}').lower()

		self._content_length()
		try:
			if resolution.lower() == 'm':
				return self.content_length[eaddr]
			elif resolution.lower() == 'y':
				y = self.content_length[eaddr].resample('AS').sum()
				y.index = y.index.year
				return y
			else:
				return None
		except KeyError:
			return None

	def content_length_overall(self):

		self._content_length()
		try:
			sum_content_length_month = self.content_length.sum(axis=1)
			if resolution.lower() == 'm':
				return sum_content_length_month
			elif resolution.lower() == 'y':
				y = sum_content_length_month.resample('AS').sum()
				y.index = y.index.year
				return y
			else:
				return None
		except:
			return None

	def content_length_from_ranking(self, resolution='M', rank=5, filter_nettime=True):
		# finish this -- re resolution
		self._content_length()
		cfr = self.content_length.sum(axis=0).order(ascending=False)
		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			cfr = cfr[cfr.index.str.contains(p)]
		return cfr[:rank]

	def plot_content_length_from_ranking(self, resolution='y', rank=5, figsize=(8, 7)):

		content_rank = self.content_length_from_ranking(rank=rank).keys()
		series = []
		for k in content_rank:
			series.append(self.content_length_from(k, resolution))
			
		df = pd.concat(series, axis=1)
		
		colors = np.random.rand(len(df),3)

		if figsize:
			df.plot(colors=colors, figsize=figsize)
		else:
			df.plot(colors=colors)

	'''
	threads
	'''			

	def _threads(self):

		if self.threads is None:
			self.threads = self.dataframe[self.dataframe['nbr-references'] > 0].reindex(columns=['from','nbr-references','subject', 'url', 'message-id']).sort_values('nbr-references', ascending=False)
		return self.threads;

	def threads_ranking(self, rank=5):

		self._threads()
		return self.threads.drop('message-id', axis=1)[:rank]


