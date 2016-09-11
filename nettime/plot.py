import numpy as np
import pandas as pd
import query

# for colormaps see: 
# http://scipy.github.io/old-wiki/pages/Cookbook/Matplotlib/Show_colormaps
# http://pandas.pydata.org/pandas-docs/stable/visualization.html#colormaps
# http://matplotlib.org/examples/color/colormaps_reference.html
# for colors see:
# http://matplotlib.org/examples/color/named_colors.html

def bar_plot_series(series, title, color='blueviolet'):
	return series.plot(kind = 'bar', title=title, color=color, alpha=0.8, stacked=True)

def save(plot, name):
	fig = plot.get_figure()
	fig.savefig(name)

class Plot:

	query = None

	def __init__(self, q=None):

		if not isinstance(q, query.Query):
			logging.error("HtmlFormat constructor Error: query must be of type nettime.query.Query")
			raise Exception()

		self.query = q

	'''
	activity
	'''					

	def activity_from_ranking(self, resolution='y', rank=5, colormap='spectral', figsize=(8, 7)):

		activity_rank = self.query.activity_from_ranking(rank=rank, series=True).keys()
		series = []
		for k in activity_rank:
			series.append(self.query.activity_from(k, resolution, series=True))
			
		df = pd.concat(series, axis=1)

		return df.plot.area(colormap='spectral', figsize=figsize, stacked=False)
		
	'''
	content lenght
	'''

	def content_length_from_ranking(self, resolution='y', rank=5, colormap='spectral', figsize=(8, 7)):

		content_rank = self.query.content_length_from_ranking(rank=rank, series=True).keys()
		series = []
		for k in content_rank:
			series.append(self.query.content_length_from(k, resolution, series=True))
			
		df = pd.concat(series, axis=1)

		return df.plot.area(colormap=colormap, figsize=figsize, stacked=False)		

	'''
	threads
	'''			

	def threads_from_ranking(self, resolution='y', rank=5, colormap='spectral', figsize=(8, 7)):

		threads_rank = self.query.threads_from_ranking(rank=rank, series=True).keys()
		series = []
		for k in threads_rank:
			series.append(self.query.threads_from(k, resolution, series=True))
			
		df = pd.concat(series, axis=1)
		
		return df.plot.area(colormap=colormap, figsize=figsize, stacked=False)
