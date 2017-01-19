import numpy as np
import pandas as pd
import archive
import logging

class Query:

	netarchive = None		# nettime.archive.Archive object
	activity = None			# (very) sparse dataframe (index=date(month), columns=from, values=activity(month))
	content_length = None	# (very) sparse dataframe (index=date(month), columns=from, values=content-length(month in bytes))
	threads = None			# ...
	replies = None			# ...

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

	def activity_from(self, email_address, resolution='y', series=False):

		eaddr = email_address.replace('@', '{at}').lower()

		freq = 'M'
		if resolution.lower() == 'y':
			freq = 'AS'
		elif resolution.lower() == 'm':
			freq = 'M'
		else:
			return None		

		self._activity()
		try:
			af = self.activity[eaddr]			
		except KeyError:
			return None

		activity_from = af.groupby([pd.TimeGrouper(freq=freq)]).sum()

		if freq == 'AS':
			activity_from.index = activity_from.index.format(formatter=lambda x: x.strftime('%Y'))
			activity_from.index.name = 'year'
		else:
			activity_from.index = activity_from.index.format(formatter=lambda x: x.strftime('%Y-%m'))
			activity_from.index.name = 'year-month'

		if series:
			return activity_from

		return activity_from.to_frame('nbr-messages').astype(int)

	def activity_from_ranking(self, rank=5, filter_nettime=True, series=False):
		
		self._activity()
		afr = self.activity.sum(axis=0).order(ascending=False)
		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			afr = afr[afr.index.str.contains(p)]

		if series:
			return afr[:rank]

		return afr[:rank].to_frame('nbr-messages').astype(int)	


	def activity_overall(self, resolution='y', series=False):

		freq = 'M'
		if resolution.lower() == 'y':
			freq = 'AS'
		elif resolution.lower() == 'm':
			freq = 'M'
		else:
			return None

		self._activity()

		y = self.activity.sum(axis=1)
		y = y.groupby([pd.TimeGrouper(freq=freq)]).sum()

		if freq == 'AS':
			y.index = y.index.format(formatter=lambda x: x.strftime('%Y'))
			y.index.name = 'year'
		else:
			y.index = y.index.format(formatter=lambda x: x.strftime('%Y-%m'))
			y.index.name = 'year-month'

		if series:
			return y

		return y.to_frame('nbr-messages').astype(int)

	def cohort(self, resolution='m', series=False):

		freq = 'M'
		if resolution.lower() == 'y':
			freq = 'AS'
		elif resolution.lower() == 'm':
			freq = 'M'
		else:
			return None

		self._activity()

		c = self.activity.idxmax().order().to_frame('date')
		c.index = c['date']

		cohort = c.groupby([pd.TimeGrouper(freq=freq)]).size()

		if freq == 'AS':
			cohort.index = cohort.index.format(formatter=lambda x: x.strftime('%Y'))
			cohort.index.name = 'year'
		else:
			cohort.index = cohort.index.format(formatter=lambda x: x.strftime('%Y-%m'))
			cohort.index.name = 'year-month'

		if series:
			return cohort

		return cohort.to_frame('first-messages').astype(int)

	'''
	content lenght
	'''

	def _content_length(self):

		if self.content_length is None:
			from_content_index = self.netarchive.dataframe.reindex(columns=['from', 'content-length'])
			self.content_length = from_content_index.groupby([pd.TimeGrouper(freq='M'), 'from']).sum()
			self.content_length = self.content_length.reset_index().pivot(columns='from', index='date', values='content-length').fillna(0)

		return self.content_length

	def content_length_from(self, email_address, resolution='y', series=False):

		eaddr = email_address.replace('@', '{at}').lower()

		freq = 'M'
		if resolution.lower() == 'y':
			freq = 'AS'
		elif resolution.lower() == 'm':
			freq = 'M'
		else:
			return None		

		self._content_length()
		try:
			af = self.content_length[eaddr]			
		except KeyError:
			return None

		content_length_from = af.groupby([pd.TimeGrouper(freq=freq)]).sum()

		if freq == 'AS':
			content_length_from.index = content_length_from.index.format(formatter=lambda x: x.strftime('%Y'))
			content_length_from.index.name = 'year'
		else:
			content_length_from.index = content_length_from.index.format(formatter=lambda x: x.strftime('%Y-%m'))
			content_length_from.index.name = 'year-month'

		if series:
			return content_length_from

		return content_length_from.to_frame('nbr-bytes').astype(int)

	def content_length_from_ranking(self, resolution='y', rank=5, filter_nettime=True, series=False):
		
		self._content_length()
		cfr = self.content_length.sum(axis=0).order(ascending=False)
		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			cfr = cfr[cfr.index.str.contains(p)]

		if series:
			return cfr[:rank]

		return cfr[:rank].to_frame('nbr-bytes').astype(int)

	def content_length_overall(self, resolution='y', series=False):

		freq = 'M'
		if resolution.lower() == 'y':
			freq = 'AS'
		elif resolution.lower() == 'm':
			freq = 'M'
		else:
			return None

		self._content_length()

		y = self.content_length.sum(axis=1)
		y = y.groupby([pd.TimeGrouper(freq=freq)]).sum()

		if freq == 'AS':
			y.index = y.index.format(formatter=lambda x: x.strftime('%Y'))
			y.index.name = 'year'
		else:
			y.index = y.index.format(formatter=lambda x: x.strftime('%Y-%m'))
			y.index.name = 'year-month'

		if series:
			return y

		return y.to_frame('nbr-bytes').astype(int)


	'''
	threads
	'''			

	def _threads(self, thresh=0):

		if self.threads is None:
			self.threads = self.netarchive.dataframe[self.netarchive.dataframe['nbr-references'] > thresh].reindex(columns=['from','nbr-references','subject', 'url', 'message-id']).sort_values('nbr-references', ascending=False)
		return self.threads;

	def threads_ranking(self, rank=5, resolution=None):

		self._threads()

		if resolution == None:
			data = self.threads.drop('message-id', axis=1)[:rank]
			return data.reindex_axis(['subject', 'from', 'nbr-references', 'url'], axis=1)

		freq = 'M'
		if resolution.lower() == 'y':
			freq = 'AS'
		elif resolution.lower() == 'm':
			freq = 'M'
		else:
			return None

		# get the threads ranking per time resolution
		# 
		data = self.threads.drop('message-id', axis=1)
		data = data.groupby([pd.TimeGrouper(freq=freq)])
		r = {}
		for k, v in data:
			if freq == 'AS':
				time_key = k.strftime('%Y')
			else:
				time_key = k.strftime('%Y-%m')
			frame = v[:rank]
			frame = frame.reindex_axis(['subject', 'from', 'nbr-references', 'url'], axis=1)
			r[time_key] = frame
		return r


	def threads_replies_to(self, email_address, resolution='y', series=False):

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
		threads_from_ranking = threads_from.groupby([pd.TimeGrouper(freq=freq), 'from']).sum()  # <-- sum = adding up nbr references 
		threads_from_ranking = threads_from_ranking.reset_index().pivot(columns='from', index='date', values='nbr-references').fillna(0)

		if series:
			return threads_from_ranking[eaddr]

		threads_from_ranking = threads_from_ranking[eaddr].to_frame('nbr-threads').astype(int)

		if freq == 'AS':
			threads_from_ranking.index = threads_from_ranking.index.format(formatter=lambda x: x.strftime('%Y'))
			threads_from_ranking.index.name = 'year'
		else:
			threads_from_ranking.index = threads_from_ranking.index.format(formatter=lambda x: x.strftime('%Y-%m'))
			threads_from_ranking.index.name = 'year-month'

		return threads_from_ranking

	def threads_replies_to_ranking(self, rank=5, filter_nettime=True):

		self._threads()

		tfr = self.threads.reindex(columns=['from', 'nbr-references']).groupby('from').sum().sort_values('nbr-references', ascending=False)

		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			tfr = tfr[tfr.index.str.contains(p)]

		tfr = tfr[:rank].astype(int)
		return tfr

	def threads_initiated_from_ranking(self, rank=5, filter_nettime=True, series=False):

		self._threads()
		tir = self.threads.reindex(columns=['from']).groupby('from').size().sort_values(ascending=False)
		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			tir = tir[tir.index.str.contains(p)]

		if series:
			return tir[:rank]

		return tir[:rank].to_frame('nbr-initiated-threads').astype(int)

	def threads_activity_threads_initiated_avg_ranking(self, rank=5, filter_nettime=True):

		# activity
		self._activity()
		afr = self.activity.sum(axis=0).astype(int)
		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			afr = afr[afr.index.str.contains(p)]

		# initiated threads [top 25]
		self._threads()
		tir = self.threads.reindex(columns=['from']).groupby('from').size().sort_values(ascending=False)[:25] # <-- top 25
		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			tir = tir[tir.index.str.contains(p)]

		inter = afr.index.intersection(tir.index)
		avg = tir[inter] / afr[inter]

		labels = ['messages', 'threads', 'avg.threads']
		return pd.concat([afr[avg.index], tir[avg.index], avg], axis=1, keys=labels).sort_values('avg.threads', ascending=False)[:rank]

	def threads_initiated_replies_avg_ranking(self, rank=5, filter_nettime=True):

		self._threads()

		#initiated
		tir = self.threads.reindex(columns=['from']).groupby('from').size().sort_values(ascending=False)
		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			tir = tir[tir.index.str.contains(p)]

		#replies [top 25]
		tfr = self.threads.reindex(columns=['from', 'nbr-references']).groupby('from').sum().sort_values('nbr-references', ascending=False)[:25] # <-- top 25
		if filter_nettime:
			p = r'^((?!nettime*).)*$'
			tfr = tfr[tfr.index.str.contains(p)]
		tfr = tfr['nbr-references']			# dataframe to series


		inter = tir.index.intersection(tfr.index)
		avg = tfr[inter] / tir[inter] 

		labels = ['threads', 'replies', 'avg.replies']
		return pd.concat([tir[avg.index], tfr[avg.index], avg], axis=1, keys=labels).sort_values('avg.replies', ascending=False)[:rank]


	def threads_overall(self, resolution='y', aggregate='sum', tresh=0):

		freq = 'M'
		if resolution.lower() == 'y':
			freq = 'AS'
		elif resolution.lower() == 'm':
			freq = 'M'
		else:
			return None

		agg = aggregate.lower()
		if not agg in ['sum', 'mean', 'count']:
			return None

		if not self.threads is None:
			del self.threads
			self.threads = None

		self._threads(tresh)

		if agg == 'sum':
			# number of replies total (re: sum all the replies)
			y = self.threads.groupby([pd.TimeGrouper(freq=freq)]).sum()
		elif agg == 'mean':
			y = self.threads.groupby([pd.TimeGrouper(freq=freq)]).mean()
		else:
			# number of threads (re: msgs with at least one reply)
			y = self.threads['nbr-references'].groupby([pd.TimeGrouper(freq=freq)]).count()
			y = y.to_frame('nbr-threads')

		if freq == 'AS':
			y.index = y.index.format(formatter=lambda x: x.strftime('%Y'))
			y.index.name = 'year'
		else:
			y.index = y.index.format(formatter=lambda x: x.strftime('%Y-%m'))
			y.index.name = 'year-month'

		return y	


	'''
	replies
	'''

	def _replies(self):

		if self.replies is None:
			self.replies = self.netarchive.dataframe[self.netarchive.dataframe['references'] > 0].reindex(columns=['from','references'])
		return self.replies;

	def replies_ranking(self, rank=5, resolution=None):

		self._replies()

		if resolution == None:
			data = self.replies.groupby('from').size().sort_values(ascending=False)[:rank]
			return data.to_frame('nbr_replies')

		freq = 'M'
		if resolution.lower() == 'y':
			freq = 'AS'
		elif resolution.lower() == 'm':
			freq = 'M'
		else:
			return None

		# get the threads ranking per time resolution
		# 
		data = self.replies.groupby([pd.TimeGrouper(freq=freq)])
		r = {}
		for k, v in data:
			if freq == 'AS':
				time_key = k.strftime('%Y')
			else:
				time_key = k.strftime('%Y-%m')
			frame = v.groupby('from').size().sort_values(ascending=False)[:rank]
			r[time_key] = frame.to_frame('nbr_replies')
		return r

	def replies_avg_ranking(self, rank=5, filter_nettime=True):

			# activity
			self._activity()
			afr = self.activity.sum(axis=0)
			if filter_nettime:
				p = r'^((?!nettime*).)*$'
				afr = afr[afr.index.str.contains(p)]

			# replies in thread [top 25]

			self._replies()
			rpl = data = self.replies.groupby('from').size().sort_values(ascending=False)[:25]

			inter = afr.index.intersection(rpl.index)
			avg = rpl[inter] / afr[inter]

			labels = ['messages', 'replies', 'avg.replies']
			return pd.concat([afr[avg.index], rpl[avg.index], avg], axis=1, keys=labels).sort_values('avg.replies', ascending=False)[:rank]



