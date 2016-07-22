import numpy as np
import pandas as pd
import archive
import logging, html

class Query:

	netarchive = None		# nettime.archive.Archive object
	activity = None			# (very) sparse dataframe (index=date(month), columns=from, values=activity(month))
	content_length = None	# (very) sparse dataframe (index=date(month), columns=from, values=content-length(month in bytes))
	threads = None			# ...

	def __init__(self, arch=None):

		if not isinstance(arch, archive.Archive):
			logging.error("Query constructor Error: arch must be of type nettime.archive.Archive")
			raise Exception()

		self.netarchive = arch

	'''
	activity
	'''			

	def _activity(self):

		if self.activity is None:
			from_index = self.netarchive.dataframe.reindex(columns=['from'])
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
				sum_activity_month.rename
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
			from_content_index = self.netarchive.dataframe.reindex(columns=['from', 'content-length'])
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

	def _threads(self, thresh=0):

		if self.threads is None:
			self.threads = self.netarchive.dataframe[self.netarchive.dataframe['nbr-references'] > thresh].reindex(columns=['from','nbr-references','subject', 'url', 'message-id']).sort_values('nbr-references', ascending=False)
		return self.threads;

	def threads_ranking(self, rank=5, output=None):

		self._threads()
		data = self.threads.drop('message-id', axis=1)[:rank]
		data['date'] = data.index
		if output is None:
			return data
		elif output == 'string':
			return data.to_string()
		elif output == 'html':
			h = html.HTML()
			t = h.table()

			r = t.tr
			r.td('date', klass='td_date_t')
			r.td('from', klass='td_from_t')
			r.td('replies', klass='td_rep_t')
			r.td('subject', klass='td_subject_t')

			for i, row in data.iterrows():
				r = t.tr
				r.td(str(row['date']), klass='td_date')
				r.td(row['from'], klass='td_from')
				r.td(str(row['nbr-references']), klass='td_rep')
				r.td('', klass='td_subject').text(str(h.a(row['subject'], href=row['url'])), escape=False)

			return str(t)
		else:
			return None

	def threads_from(self, email_address, resolution='y'):

		freq = 'M'
		if resolution.lower() == 'y':
			freq = 'AS'
		elif resolution.lower() == 'm':
			freq = 'M'
		else:
			return None

		self._threads()

		eaddr = email_address.replace('@', '{at}').lower()

		self._threads()
		threads_from = self.threads.reindex(columns=['from', 'nbr-references'])
		threads_from_ranking = threads_from.groupby([pd.TimeGrouper(freq=freq), 'from']).sum()
		threads_from_ranking = threads_from_ranking.reset_index().pivot(columns='from', index='date', values='nbr-references').fillna(0)
		return threads_from_ranking[eaddr]

	def threads_from_ranking(self, rank=5, filter_nettime=True):

		self._threads()
		threads_from = self.threads.reindex(columns=['from', 'nbr-references'])
		threads_from_ranking = threads_from.groupby([pd.TimeGrouper(freq='AS'), 'from']).sum()
		threads_from_ranking = threads_from_ranking.reset_index().pivot(columns='from', index='date', values='nbr-references').fillna(0)
		tfr = threads_from_ranking.sum(axis=0).order(ascending=False)

		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			tfr = tfr[tfr.index.str.contains(p)]

		return tfr[:rank]

	def plot_threads_from_ranking(self, resolution='y', rank=5, figsize=(8, 7)):

		threads_rank = self.threads_from_ranking(rank=rank).keys()
		series = []
		for k in threads_rank:
			series.append(self.threads_from(k, resolution))
			
		df = pd.concat(series, axis=1)
		
		colors = np.random.rand(len(df),3)

		if figsize:
			df.plot(colors=colors, figsize=figsize)
		else:
			df.plot(colors=colors)


	def threads_overall(self, resolution='y', aggregate='sum', tresh=0):

		freq = 'M'
		if resolution.lower() == 'y':
			freq = 'AS'
		elif resolution.lower() == 'm':
			freq = 'M'
		else:
			return None

		agg = aggregate.lower()
		if not agg in ['sum', 'mean']:
			return None

		if not self.threads is None:
			del self.threads
			self.threads = None

		self._threads(tresh)

		if agg == 'sum':
			y = self.threads.groupby([pd.TimeGrouper(freq=freq)]).sum()
		else:
			y = self.threads.groupby([pd.TimeGrouper(freq=freq)]).mean()

		if freq == 'AS':
			y.index = y.index.year

		return y			
