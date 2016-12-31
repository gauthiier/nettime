import sys, os, json, logging
from optparse import OptionParser

reload(sys)
sys.setdefaultencoding('utf8')

logging.info('1/4 setting up matplotlib')
# matplot view/windows
import matplotlib
import matplotlib.pyplot as plt
matplotlib.interactive(True)

logging.info('2/4 setting up pandas')
# pd display
import pandas as pd
pd.set_option('display.max_colwidth', 100)

logging.info('3/4 loading nettime archive')
import nettime.archive
import nettime.query
import nettime.report

a = nettime.archive.Archive('nettime-l_2016-12-31.json.gz')
q = nettime.query.Query(a)
r = nettime.report.Report(q)

logging.info('4/4 reporting')

def text(command, params=None):

	print command

	func = {
		"tab_msgs_threads_replies": r.tab_msgs_threads_replies,
		"tab_avg_rep_msg_thrd": r.tab_avg_rep_msg_thrd,
		"tab_activity_from_ranking": r.tab_activity_from_ranking,
		"tab_content_length_from_ranking": r.tab_content_length_from_ranking,
		"tab_threads_ranking": r.tab_threads_ranking,
		"tab_threads_ranking_year": r.tab_threads_ranking_year
	}

	print func[command]

	return func[command]()

def html(command, params=None):

	func = {
		"html_msgs_threads_replies": r.html_msgs_threads_replies,
		"html_avg_rep_msg_thrd": r.html_avg_rep_msg_thrd,
		"html_activity_from_ranking": r.html_activity_from_ranking,
		"html_content_length_from_ranking": r.html_content_length_from_ranking,
		"html_threads_ranking": r.html_threads_ranking,
		"html_threads_ranking_year": r.html_threads_ranking_year
	}

	return func[command]()

def run(options):

	if options.output_file and os.path.isfile(options.output_file):
		with open(options.output_file, 'r') as fp:
			out = fp.read()				# not optimal but will do
	else:
		print 'No output-file. Nothing to do.'
		return

	if options.input_script and os.path.isfile(options.input_script):
		with open(options.input_script, 'r') as fp:
			input_script = json.load(fp)
	else:
		print 'No input-script. Nothing to do.'
		return

	for cmd in input_script:

		if cmd['format'] == 'html':
			func = html
		elif cmd['format'] == 'text':
			func = text
		else:
			continue

		res = func(cmd['command'])

		if res is not None: 			
			out = out.replace(cmd['replace'], res)

	with open(options.output_file, 'w') as fp:
		fp.write(out)				# not optimal but will do


if __name__ == "__main__":

    p = OptionParser();
    p.add_option('-i', '--input-script', action="store", help="..")
    p.add_option('-o', '--output-file', action="store", help="..")

    options, args = p.parse_args()

    run(options)


