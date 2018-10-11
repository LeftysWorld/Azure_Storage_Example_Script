"""
1337 DEVELOPER NOTES:
    RUN SCRIPTS:
        MAKE SURE:
            - to update blob_tracker.txt and date_tracker.txt after any git pull.
        run index.py script in background - move old 'index_out' to 'oldLogFiles' and rename for dates processed
            nohup python index.py >index_out.txt 2>&1 &
        run normalize_data_call.py in background - move old 'normalize_out.txt' to 'oldLogFiles' and rename for dates processed
            nohup python normalize_data_call.py >normalize_out.txt 2>&1 &

    AFTER STOP SCRIPTS:
        - do manually, not programatically
        index.py:
            rm -rf blob_dump/*/*
                - removes old files from blob_dump/:
                - do manually, not programatically
            rm -rf concat_data.csv daily_data/*
                - removes old files from proc_files_dir/:

    (UN)PAUSE INDEX.PY:
        pause index.py:
            touch /XXXX/XXXX/XXXX/bcppause.txt
        unpause index.py
            rm /XXXX/XXXX/XXXX/bcppause.txt
"""

from azure.storage.blob import BlockBlobService
from collections import defaultdict
import os
import re
import subprocess
import time
import datetime
import shlex
import pandas as pd

from config import (
    STORAGE_ACCOUNT_NAME,
    ACCESS_KEY,
    CONTAINER_NAME,
    AZURE_DATABASE_LOCATION,
    AZURE_DATABASE_NAME,
    BLOB_LOCAL_LOCATION,
    SQL_PASSWORD,
    SQL_USERNAME
)


def block_blob_naming(loc, name, ending=''):
    return "{0}/{1}{2}".format(loc, name, ending)


def zname(zipf):
    # get filename less file format
    try:
        return zipf.filename.replace('.zip', '')
    except Exception:
        return zipf.replace('.zip', '')


def zopen(blobname, _blob_year):
    # unzip via subprocess. Zip files are too large to use vanilla python
    pathToZip = block_blob_naming(BLOB_LOCAL_LOCATION, blobname, ending='.zip')
    pathToOut = block_blob_naming(BLOB_LOCAL_LOCATION, blobname, ending='/')
    pathToFixed = re.sub('(.*[/])(.*?).zip$', lambda m: '{}{}_fixed.zip'.format(*m.groups()), pathToZip)
    fix_cmd = 'zip -FF {} --out {} >/dev/null'.format(pathToZip, pathToFixed)
    subprocess.call(fix_cmd, shell=True)
    os.remove(pathToZip)
    unzip_cmd = 'unzip -o {} -d {} >/dev/null'.format(pathToFixed, pathToOut)
    subprocess.call(unzip_cmd, shell=True)
    os.remove(pathToFixed)


def cleandir(dname, clrall=False):
    # remove files after use
    if clrall is True:
        for p, ds, fs in os.walk(dname):
            for f in fs:
                os.unlink(os.path.join(p, f))
            for d in ds:
                next_d = os.path.join(p, d)
                cleandir(next_d, clrall=True)
                os.rmdir(next_d)

    for p, ds, fs in os.walk(dname):
        for f in fs:
            if f.startswith('__MAC') or f.endswith('.zip'):
                os.unlink(os.path.join(p, f))
        for d in ds:
            if d.startswith('__MAC'):
                next_d = os.path.join(p, d)
                cleandir(next_d, clrall=True)
                os.rmdir(next_d)


def resume_point(_file):
    # Look at previous processed blobs/dates and skip those
    tracker_file = open(_file, 'r')
    tracker = tracker_file.readlines()
    # Previous processed blob/date
    left_off_point = int(tracker[-1])
    tracker_file.close()
    return left_off_point


def handle_completion_point(_file, _point):
    with open(_file, 'a') as tracker_file:
        tracker_file.write("\n{}".format(_point))
        tracker_file.close()
    return


def sort_files(blobname):
    print("\t PROCESSING: {}".format(blobname.replace("{}".format(BLOB_LOCAL_LOCATION), "")))
    # Dictionary to hold data //=> csv_by_date = {'20120514': [csv1, csv2, csvn], '20120515': [csv1, csv2, csvn]}
    csv_by_date = defaultdict(list)

    for root, dirs, files in os.walk(blobname):
        # Loop through csv files in 'XXXX/blob_dump/{blob_year}/{blob_month}/'
        # Example fname = 'XXXXX.csv'
        for fname in sorted(files):
            basename, extension = fname.split('.')
            # {current: 'current', loc: 'XXXXX', timestamp: '2012051416241294'}
            current, loc, timestamp = basename.split('_')
            # timestamp_date = '20120514'
            timestamp_date = timestamp[0:8]
            if csv_by_date[timestamp_date] not in csv_by_date[timestamp_date]:
                # Example: csv_by_date['20120514'].append('blob_dump/2012/05/XXXXX.csv')
                csv_by_date[timestamp_date].append(os.path.join(root, fname))

    # Convert csv_by_date into tuple to order by date in ascending order
    # Example ((20120514, [csv_file_lst]), (2020515, [csv_file_lst]), ...)
    sorted_csv_files = sorted(csv_by_date.iteritems(), key=lambda (k, v): v[0], reverse=False)
    return sorted_csv_files


def handle_csv_bcp(_date_data_csv):
    def get_cmd_fn(_csv):
        last_header_name = pd.read_csv(_csv, sep="|").columns.get_values()[-1]
        table = "XXXXXXX"

        if last_header_name == "XXXXXXX":
            table = "XXXXXXX"
        elif last_header_name == "XXXXXXX":
            table = "XXXXXXX"
        elif last_header_name == "DATA_PROVIDER_ID":
            table = "XXXXXXX"
        elif last_header_name == "DATA_PROVIDER_ICON":
            table = "XXXXXXX"
        elif last_header_name == "DATA_PROVIDER_TEXT":
            table = "XXXXXXX"

        _cmd = 'bcp [{0}].Staging.{1} in {2} -S tcp:{3} -U {4} -c -t"|" -F 2 -XXXXXXX -XXXXXXX'.format(
            AZURE_DATABASE_NAME,
            table,
            csv,
            AZURE_DATABASE_LOCATION,
            SQL_USERNAME
        )
        return _cmd

    for date, data in _date_data_csv:
        date_tracker_file = 'date_tracker.txt'
        if int(date) > resume_point(date_tracker_file):

            for csv in data:
                print("\t\t\t CSV BCP: {}".format(csv))
                cmd = get_cmd_fn(csv)
                pswd = str.encode('{}\n'.format(SQL_PASSWORD))
                proc = subprocess.Popen(shlex.split(cmd), stdin=subprocess.PIPE, stdout=subprocess.PIPE)
                proc.stdin.write(pswd)
                out, err = proc.communicate()

                if err is not None:
                    print("{}".format(err))

            handle_completion_point(date_tracker_file, date)


def main():
    block_blob_service = BlockBlobService(account_name=STORAGE_ACCOUNT_NAME, account_key=ACCESS_KEY)
    generator = block_blob_service.list_blobs(CONTAINER_NAME)
    blob_tracker_file = 'blob_tracker.txt'

    for blob in generator:
        start_time = datetime.datetime.now().strftime("20%y/%m/%d %H:%M")
        print("START: {0} @ {1}".format(zname(blob.name), start_time))

        # Get year and month from blob.name
        blob_name, blob_type = blob.name.split(".")
        blob_year, blob_month = blob_name.split("/")
        full_blob_path = block_blob_naming(BLOB_LOCAL_LOCATION, blob.name)
        current_blob = "{0}{1}".format(blob_year, blob_month)

        if int(current_blob) > resume_point(blob_tracker_file):
            timer_start = time.time()
            # Download blob from generator to blob_dump/{year}/{month}
            block_blob_service.get_blob_to_path(CONTAINER_NAME, blob.name, full_blob_path)
            timer_after_download = time.time()
            print("\t DOWNLOAD TIME: {}".format(timer_after_download - timer_start))

            # Unzips the zip file
            zopen(blob_name, blob_year)
            timer_after_unzip = time.time()
            print("\t UNZIP TIME: {}".format(timer_after_unzip - timer_after_download))

            # Sort Blob CSVs by Date
            sorted_blob_csv_by_date = sort_files(zname(full_blob_path))
            # BCP each individual file
            handle_csv_bcp(sorted_blob_csv_by_date)
            # Cleans up folder after processing
            cleandir(zname(full_blob_path), clrall=True)

            # "Log" this blob as completed
            handle_completion_point(blob_tracker_file, current_blob)

            timer_end = time.time()
            print("\t PROCESSING TIME: {}".format(timer_end - timer_after_unzip))
            print("FINISHED {0} IN {1}".format(blob.name, timer_end - timer_start))
            print("")

    else:
        print("COMPLETED Hurray!!!")


if __name__ == '__main__':
    main()
