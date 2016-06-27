import sys, logging
from optparse import OptionParser
import lib.nettime as nettime

logging.basicConfig(level=logging.DEBUG)

def run(options):

    if not options.url:
        sys.exit('No url. Aborting.')

    if not options.list:
        sys.exit('No list. Aborting.')

    ## check valid url?... nej

    nettime.archive_from_url(options.url, options.list, options.arch)
    sys.exit()

if __name__ == "__main__":

    p = OptionParser();
    p.add_option('-u', '--url', action="store", help="nettime url", default="http://www.nettime.org/archives.php")
    p.add_option('-l', '--list', action="store", help="nettime's list name (ex: nettime-l)", default="nettime-l")
    p.add_option('-a', '--arch', action="store", help="path to archive directory", default="archives")

    options, args = p.parse_args()

    run(options)
