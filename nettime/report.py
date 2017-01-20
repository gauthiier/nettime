import query
import format
import plot

class Report:

	query = None
	matrix = None

	def __init__(self, q=None):

		if not isinstance(q, query.Query):
			logging.error("HtmlFormat constructor Error: query must be of type nettime.query.Query")
			raise Exception()

		self.query = q

	'''
	(basic) stats
	'''

	def matrix_msgs_threads(self):

		if self.matrix is None:

			# nbr messages
			mat = self.query.activity_overall()

			# nbr threads
			mat['nbr-threads'] = self.query.threads_overall(aggregate='count')['nbr-threads']

			# nbr replies
			mat['nbr-replies'] = self.query.threads_overall(aggregate='sum')['nbr-references']

			# nbr non-replies (aka. non-threads)
			mat['nbr-single-messages'] = mat['nbr-messages'] - mat['nbr-replies'] - mat['nbr-threads']

			# avg. threads per message
			mat['avg-thrd-per-msg'] = mat['nbr-threads'] / mat['nbr-messages']

			# avg. replies per message
			mat['avg-rep-per-msg'] = mat['nbr-replies'] / mat['nbr-messages']			

			# avg. threadss + replies per message
			mat['avg-thrd-rep-per-msg'] = (mat['nbr-threads'] + mat['nbr-replies']) / mat['nbr-messages']			

			# avg. threadss + replies per message
			mat['diff-thrd-rep-vs-single-msg'] = (mat['nbr-threads'] + mat['nbr-replies']) - mat['nbr-single-messages']			

			# avg. rep per thread
			mat['avg-rep-per-thrd'] = mat['nbr-replies'] / mat['nbr-threads']	
			# same as:
			# mat['avg-rep-per-thrd'] = q.threads_overall(aggregate='mean')['nbr-references']



			self.matrix = mat

		return self.matrix

	'''
	plots
	'''

	def plot_nbr_msgs(self, title='Nbr. Messages', label='messages', color='mediumblue'):

		self.matrix_msgs_threads()

		return plot.bar_plot_series(self.matrix['nbr-messages'].to_frame(label), title=title, color=color)

	def plot_nbr_threads(self, title='Nbr. Threads', label='threads', color='crimson'):

		self.matrix_msgs_threads()

		return plot.bar_plot_series(self.matrix['nbr-threads'].to_frame(label), title=title, color=color)

	def plot_nbr_replies(self, title='Nbr. Replies in Threads', label='replies', color='dimgray'):

		self.matrix_msgs_threads()

		return plot.bar_plot_series(self.matrix['nbr-replies'].to_frame(label), title=title, color=color)

	def plot_avg_thread_p_msg(self, title='Avg. Threads', label='', color='crimson'):

		self.matrix_msgs_threads()

		return plot.bar_plot_series(self.matrix['avg-thrd-per-msg'].to_frame(label), title=title, color=color)

	def plot_avg_replies_p_msg(self, title='Avg. Replies', label='', color='dimgray'):

		self.matrix_msgs_threads()

		return plot.bar_plot_series(self.matrix['avg-rep-per-msg'].to_frame(label), title=title, color=color)

	def plot_avg_threads_replies_p_msg(self, title='Avg. Threads + Replies', label='avg', color='crimson'):

		self.matrix_msgs_threads()

		return plot.bar_plot_series(self.matrix['avg-thrd-rep-per-msg'].to_frame(label), title=title, color=color)

	def plot_diff_threads_replies_v_single_msg(self, title='Diff. Threads + Replies vs Single Messages', label='diff', color='b'):

		self.matrix_msgs_threads()

		return plot.bar_plot_series(self.matrix['diff-thrd-rep-vs-single-msg'].to_frame(label), title=title, color=color)


	def plot_avg_rep_p_thrd(self, title='Ratio Replies per Thread', label='replies-per-thread', color='blueviolet'):

		self.matrix_msgs_threads()

		return plot.bar_plot_series(self.matrix['avg-rep-per-thrd'].to_frame(label), title=title, color=color)

	def plot_msgs_replies(self, title='Messages Constituency'):

		self.matrix_msgs_threads()

		return plot.bar_plot_series(self.matrix[['nbr-single-messages', 'nbr-threads', 'nbr-replies']], color=['b', 'crimson', 'dimgray'], title=title)

	def plot_cohort(self, title='Cohorts'):

		c = self.query.cohort()

		return c.plot(color='b', title=title)

	'''
	text (tabular)
	'''

	def tab_msgs_threads_replies(self):
		self.matrix_msgs_threads()
		return format.Tab.from_dataframe(self.matrix[['nbr-messages', 'nbr-threads', 'nbr-replies']], 
			name_map={'nbr-messages': 'messages', 'nbr-threads': 'threads', 'nbr-replies': 'replies in threads'})

	def tab_avg_rep_msg_thrd(self):
		self.matrix_msgs_threads()
		return format.Tab.from_dataframe(self.matrix[['avg-thrd-per-msg', 'avg-rep-per-thrd']], 
			name_map={'avg-thrd-per-msg': 'avg. thread per message', 'avg-rep-per-thrd': 'avg. replies per thread'})

	def tab_msgs_threads_replies_avg_rep_msg_thrd(self):
		self.matrix_msgs_threads()
		return format.Tab.from_dataframe(self.matrix[['nbr-messages', 'nbr-threads', 'nbr-replies', 'avg-thrd-per-msg', 'avg-rep-per-thrd']], 
			name_map={'nbr-messages': 'messages', 'nbr-threads': 'threads', 'nbr-replies': 'replies in threads', 'avg-thrd-per-msg': 'avg. thread per message', 'avg-rep-per-thrd': 'avg. replies per thread'})

	def tab_activity_from_ranking(self, rank=5):
		d = self.query.activity_from_ranking(rank=rank)
		return format.Tab.from_dataframe(d, name_map={'nbr-messages': 'messages'})

	#
	def tab_threads_replies_to_ranking(self, rank=10):
		d = self.query.threads_replies_to_ranking(rank=rank)
		return format.Tab.from_dataframe(d)		

	#
	def tab_threads_initiated_from_ranking(self, rank=10):
		d = self.query.threads_initiated_from_ranking(rank=rank)
		return format.Tab.from_dataframe(d)		

	#
	def tab_threads_activity_threads_initiated_avg_ranking(self, rank=10):
		d = self.query.threads_activity_threads_initiated_avg_ranking(rank=rank)
		return format.Tab.from_dataframe(d)		

	#
	def tab_threads_initiated_replies_avg_ranking(self, rank=10):
		d = self.query.threads_initiated_replies_avg_ranking(rank=rank)
		return format.Tab.from_dataframe(d)				

	def tab_content_length_from_ranking(self, rank=5):
		d = self.query.activity_from_ranking(rank=rank)
		return format.Tab.from_dataframe(d, name_map={'nbr-bytes': 'bytes'})

	def tab_threads_ranking(self, rank=5):
		d = self.query.threads_ranking(rank=rank)
		return format.Tab.from_dataframe(d, name_map={'nbr-references': 'nbr. replies'})

	def tab_threads_ranking_year(self, rank=5, resolution='y'):
		d = self.query.threads_ranking(rank=rank, resolution=resolution)
		years = sorted(d)
		nl = '\n'
		s = ""
		for i in years:
			s += 'year: ' + i + nl
			s += format.Tab.from_dataframe(d[i], name_map={'nbr-references': 'nbr. replies'}) + nl
		return s + nl

	def tab_replies_ranking(self, rank=10):
		d = self.query.replies_ranking(rank=rank)
		return format.Tab.from_dataframe(d, name_map={'nbr_replies': 'nbr. replies'})

	def tab_replies_avg_ranking(self, rank=10):
		d = self.query.replies_avg_ranking(rank=rank)
		return format.Tab.from_dataframe(d, name_map={'nbr-replies': 'nbr. replies'})				

	'''
	html
	'''
	def html_msgs_threads_replies(self):
		self.matrix_msgs_threads()
		return format.Html.from_dataframe(self.matrix[['nbr-messages', 'nbr-threads', 'nbr-replies']], 
			name_map={'nbr-messages': 'messages', 'nbr-threads': 'threads', 'nbr-replies': 'replies in threads'})

	def html_avg_rep_msg_thrd(self):
		self.matrix_msgs_threads()
		return format.Html.from_dataframe(self.matrix[['avg-thrd-per-msg', 'avg-rep-per-thrd']], 
			name_map={'avg-thrd-per-msg': 'avg. thread per message', 'avg-rep-per-thrd': 'avg. replies per thread'})

	def html_msgs_threads_replies_avg_rep_msg_thrd(self):
		self.matrix_msgs_threads()
		return format.Html.from_dataframe(self.matrix[['nbr-messages', 'nbr-threads', 'nbr-replies', 'avg-thrd-per-msg', 'avg-rep-per-thrd']], 
			name_map={'year': 'Year', 'nbr-messages': 'Messages', 'nbr-threads': 'Threads', 'nbr-replies': 'Replies', 'avg-thrd-per-msg': 'Avg. Threads', 'avg-rep-per-thrd': 'Ratio Replies per Thread'})

	def html_activity_from_ranking(self, rank=10):
		d = self.query.activity_from_ranking(rank=rank)
		return format.Html.from_dataframe(d, name_map={'from': 'From', 'nbr-messages': 'Messages'})		

	#
	def html_threads_replies_to_ranking(self, rank=10):
		d = self.query.threads_replies_to_ranking(rank=rank)
		return format.Html.from_dataframe(d, name_map={'from': 'From', 'nbr-references': 'Replies'})		

	#
	def html_threads_initiated_from_ranking(self, rank=10):
		d = self.query.threads_initiated_from_ranking(rank=rank)
		return format.Html.from_dataframe(d, name_map={'from': 'From', 'nbr-initiated-threads': '(Initiated) Threads)'})		

	#
	def html_threads_activity_threads_initiated_avg_ranking(self, rank=10):
		d = self.query.threads_activity_threads_initiated_avg_ranking(rank=rank)
		return format.Html.from_dataframe(d, name_map={'from': 'From', 'messages': 'Messages', 'threads': 'Threads', 'avg.threads': 'Avg. Threads'})		

	#
	def html_threads_initiated_replies_avg_ranking(self, rank=10):
		d = self.query.threads_initiated_replies_avg_ranking(rank=rank)
		return format.Html.from_dataframe(d, name_map={'from': 'From', 'threads': 'Threads', 'replies': 'Replies', 'avg.replies': 'Avg. Replies'})				

	def html_content_length_from_ranking(self, rank=10):
		d = self.query.activity_from_ranking(rank=rank)
		return format.Html.from_dataframe(d, name_map={'nbr-bytes': 'bytes'})

	def html_threads_ranking(self, rank=10):
		d = self.query.threads_ranking(rank=rank)
		return format.Html.from_dataframe(d, name_map={'date': 'Date', 'subject': 'Subject', 'from': 'From', 'nbr-references': 'Replies'}, url_map={'Subject': 'url'})

	def html_threads_ranking_year(self, rank=5, resolution='y'):
		d = self.query.threads_ranking(rank=rank, resolution=resolution)
		years = sorted(d)
		nl = '\n'
		s = ""
		for i in years:
			s += '<div class="thread_rank_year">' + nl	
			s += '<div class="year_t">' + i + '</div>' + nl
			s += '<div class="rank_t">' + nl
			s += format.Html.from_dataframe(d[i], name_map={'date': 'Date', 'subject': 'Subject', 'from': 'From', 'nbr-references': 'Replies'}, url_map={'Subject': 'url'}) + nl
			s += '</div>' + nl
			s += '</div>' + nl
		return s + nl

	def html_replies_ranking(self, rank=10):
		d = self.query.replies_ranking(rank=rank)
		return format.Html.from_dataframe(d, name_map={'from': 'From', 'nbr_replies': 'Replies'})

	def html_replies_avg_ranking(self, rank=10):
		d = self.query.replies_avg_ranking(rank=rank)
		return format.Html.from_dataframe(d, name_map={'from': 'From', 'messages': 'Messages', 'replies': 'Replies', 'avg.replies': 'Avg. Replies'})				


