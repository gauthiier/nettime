import nettime.query
import nettime.format
import nettime.plot

class Report:

	query = None
	matrix = None

	def __init__(self, q=None):

		if not isinstance(q, nettime.query.Query):
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

			# avg. rep per message
			mat['avg-rep-per-msg'] = mat['nbr-replies'] / mat['nbr-messages']

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

		nettime.plot.bar_plot_series(self.matrix['nbr-messages'].to_frame(label), title=title, color=color)

	def plot_nbr_threads(self, title='Nbr. Threads', label='threads', color='crimson'):

		self.matrix_msgs_threads()

		nettime.plot.bar_plot_series(self.matrix['nbr-threads'].to_frame(label), title=title, color=color)

	def plot_nbr_replies(self, title='Nbr. Replies in Threads', label='replies', color='dimgray'):

		self.matrix_msgs_threads()

		nettime.plot.bar_plot_series(self.matrix['nbr-replies'].to_frame(label), title=title, color=color)

	def plot_avg_rep_p_msg(self, title='Avg. Replies per Messages', label='replies-per-messasges', color='limegreen'):

		self.matrix_msgs_threads()

		nettime.plot.bar_plot_series(self.matrix['avg-rep-per-msg'].to_frame(label), title=title, color=color)

	def plot_avg_rep_p_thrd(self, title='Avg. Replies per Thread', label='replies-per-thread', color='blueviolet'):

		self.matrix_msgs_threads()

		nettime.plot.bar_plot_series(self.matrix['avg-rep-per-thrd'].to_frame(label), title=title, color=color)

	def plot_msgs_replies(self, title='Nbr. Messages segments (individual messages vs thread replies)'):

		self.matrix_msgs_threads()

		nettime.plot.bar_plot_series(self.matrix[['nbr-single-messages', 'nbr-threads', 'nbr-replies']], color=['mediumblue', 'red', 'dimgray'], title=title)


