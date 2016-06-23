import urllib2, urllib, urlparse
import os, re, json, gzip
import mhonarccrawl
import datetime

def archive_from_url(url, sublist_name="nettime-l", archive_dir="archives"):
    url = url.rstrip()
    archive_list_dir = check_dir(archive_dir, sublist_name)

    archive_name = sublist_name.lower()
    archive_date = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    archive = {'name' : sublist_name.lower(), 'url': url, 'date': archive_date, 'threads' : []}

    archive['threads'] = mhonarccrawl.collect_from_url(url, sublist_name, archive_list_dir, mbox=True)

    file_path = os.path.join(archive_dir, archive_name + "_" + archive_date + ".json.gz")
    with gzip.open(file_path, 'w') as fp:
        json.dump(archive, fp, indent=4)

    return

def check_dir(base_dir, list_name):
    arc_dir = os.path.join(base_dir, list_name)
    if not os.path.exists(arc_dir):
        os.makedirs(arc_dir)
    return arc_dir