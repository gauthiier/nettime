import sys, os, json, logging
from optparse import OptionParser

reload(sys)
sys.setdefaultencoding('utf8')

# matplot view/windows
import matplotlib
matplotlib.interactive(True)

# pd display
import pandas as pd
pd.set_option('display.max_colwidth', 100)

import nettime.archive
import nettime.query
import nettime.report

class ReportDispatch:

	def __init__(self, r=None):

		if not isinstance(r, nettime.report.Report):
			logging.error("Rep constructor Error: r be of type nettime.report.Report")
			raise Exception()

		self.r = r

	def text(self, command, params=None):

		func = {
			"tab_msgs_threads_replies": self.r.tab_msgs_threads_replies,
			"tab_avg_rep_msg_thrd": self.r.tab_avg_rep_msg_thrd,
			"tab_activity_from_ranking": self.r.tab_activity_from_ranking,
			"tab_threads_replies_to_ranking": self.r.tab_threads_replies_to_ranking,
			"tab_threads_initiated_from_ranking": self.r.tab_threads_initiated_from_ranking,
			"tab_threads_activity_threads_initiated_avg_ranking": self.r.tab_threads_activity_threads_initiated_avg_ranking,
			"tab_threads_initiated_replies_avg_ranking": self.r.tab_threads_initiated_replies_avg_ranking,			
			"tab_content_length_from_ranking": self.r.tab_content_length_from_ranking,
			"tab_threads_ranking": self.r.tab_threads_ranking,
			"tab_threads_ranking_year": self.r.tab_threads_ranking_year,
			"tab_msgs_threads_replies_avg_rep_msg_thrd": self.r.tab_msgs_threads_replies_avg_rep_msg_thrd,
			"tab_replies_ranking": self.r.tab_replies_ranking,
			"tab_replies_avg_ranking": self.r.tab_replies_avg_ranking
		}

		return func[command]()

	def html(self, command, params=None):

		func = {
			"html_msgs_threads_replies": self.r.html_msgs_threads_replies,
			"html_avg_rep_msg_thrd": self.r.html_avg_rep_msg_thrd,
			"html_activity_from_ranking": self.r.html_activity_from_ranking,
			"html_threads_replies_to_ranking": self.r.html_threads_replies_to_ranking,
			"html_threads_initiated_from_ranking": self.r.html_threads_initiated_from_ranking,
			"html_threads_activity_threads_initiated_avg_ranking": self.r.html_threads_activity_threads_initiated_avg_ranking,
			"html_threads_initiated_replies_avg_ranking": self.r.html_threads_initiated_replies_avg_ranking,
			"html_content_length_from_ranking": self.r.html_content_length_from_ranking,
			"html_threads_ranking": self.r.html_threads_ranking,
			"html_threads_ranking_year": self.r.html_threads_ranking_year,
			"html_msgs_threads_replies_avg_rep_msg_thrd": self.r.html_msgs_threads_replies_avg_rep_msg_thrd,
			"html_replies_ranking": self.r.html_replies_ranking,
			"html_replies_avg_ranking": self.r.html_replies_avg_ranking
		}

		return func[command]()

def run(options):

	if options.input_script and os.path.isfile(options.input_script):
		with open(options.input_script, 'r') as fp:
			input_script = json.load(fp)
	else:
		print 'No input script. Nothing to do.'
		return

	if options.template_file and os.path.isfile(options.template_file):
		with open(options.template_file, 'r') as fp:
			out = fp.read()				# not optimal but will do
	else:
		print 'No template file. Nothing to do.'
		return

	if os.path.isfile(options.archive): 
		path, file = os.path.split(options.archive)
		a = nettime.archive.Archive(data=file, archive_dir=path)
	else:
		a = nettime.archive.Archive(options.archive)
	
	q = nettime.query.Query(a)
	r = nettime.report.Report(q)

	rep = ReportDispatch(r)

	for cmd in input_script:

		if cmd['format'] == 'html':
			res = rep.html(cmd['command'])
		elif cmd['format'] == 'text':
			res = rep.text(cmd['command'])
		else:
			continue

		if res is not None: 			
			out = out.replace(cmd['replace'], res)

	with open(options.output_file, 'w') as fp:
		fp.write(out)				# not optimal but will do


if __name__ == "__main__":

    p = OptionParser();
    p.add_option('-i', '--input-script', action="store", help="input (json) script mapping commands to text placeholders")
    p.add_option('-o', '--output-file', action="store", help="report file to be generated")
    p.add_option('-t', '--template-file', action="store", help="template file from which the report is generated")
    p.add_option('-a', '--archive', action="store", help="the archive dir or file (.json.gz) to produce the report from (default='nettime-l_2016-12-31.json.gz')", default="nettime-l_2016-12-31.json.gz")

    options, args = p.parse_args()

    if options.input_script is None:
    	p.print_help()
    	p.error('No input file specified.')    	

    if options.output_file is None:
    	p.print_help()
    	p.error('No output file specified.')    	

    if options.template_file is None:
    	p.print_help()
    	p.error('No template file specified.')    	

    run(options)


