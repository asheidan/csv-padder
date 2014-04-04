#!/usr/bin/env python

# Imports libraries so I don't have to do everything myself
import csv
from datetime import datetime, timedelta, time
from sys import argv, stderr
import os

def datetime_from_fields(fields):
    """ Reads the first two fields of a row and returns a datetime object
        representing that time.
    """

    # Extract and concatenate the first two fields
    date_string = " ".join(fields[0:2])
    # Parse the resulting string
    timestamp = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")

    return timestamp


def write_pad_rows_until_date(from_timestamp, to_timestamp, delta, csvwriter):
    current_timestamp = from_timestamp
    while current_timestamp < to_timestamp:
        csvwriter.writerow((current_timestamp, None, None))
        current_timestamp = current_timestamp + delta


start_timestamp = datetime(1961, 1, 1)
end_timestamp = datetime(2014, 1, 1)

# Tell the csv-library how the datafiles looks
csv.register_dialect('mikaeldata', delimiter=';', quoting=csv.QUOTE_MINIMAL)

for file_name in argv[1:]:
    stderr.write("%s\n" % file_name)

    with open(file_name) as csvfile:
        # Create a csv reader which reads from the opened file
        headers = None

        csvreader = csv.reader(csvfile, 'mikaeldata')
        for row in csvreader:
            if row and "Datum" == row[0]:
                headers = row
                break

        # Let's assume that the two first measurements are consequtive
        row_1 = csvreader.next()
        row_2 = csvreader.next()

        datetime_1 = datetime_from_fields(row_1)
        datetime_2 = datetime_from_fields(row_2)

        # Get the difference between measurements
        delta = datetime_2 - datetime_1
        print delta
        
        # Try to handle the different kind of datafiles
        if datetime_1.time() == time(6, 0):
            current_timestamp = start_timestamp + timedelta(hours=6)
        else:
            current_timestamp = start_timestamp

        output_filename = 'Padded Data/%s' % file_name

        # Create the directory for the outputfile if it doesn't exist
        output_dirname = os.path.dirname(output_filename)
        if not os.path.exists(output_dirname):
            os.makedirs(output_dirname)

        with open(output_filename, 'w') as outputfile:
            csvwriter = csv.writer(outputfile, 'mikaeldata')

            # Write headers to the file if we have read them
            if headers:
                csvwriter.writerow([headers[i] for i in [0,2,3]])

            # Write the previous read rows
            write_pad_rows_until_date(current_timestamp, datetime_1, delta,
                                      csvwriter)
            csvwriter.writerow((datetime_1, float(row_1[2]), row_1[3]))
            csvwriter.writerow((datetime_2, float(row_2[2]), row_2[3]))

            # Write the rest of the rows
            current_timestamp = datetime_2 + delta
            for row in csvreader:
                row_timestamp = datetime_from_fields(row)
                value = float(row[2])
                quality = row[3]
                write_pad_rows_until_date(current_timestamp, row_timestamp,
                                          delta, csvwriter)

                csvwriter.writerow((row_timestamp, value, quality))

                current_timestamp = row_timestamp + delta

            write_pad_rows_until_date(current_timestamp, end_timestamp, delta,
                                      csvwriter)
