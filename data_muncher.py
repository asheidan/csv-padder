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


def write_pad_rows_until_date(from_timestamp, to_timestamp, deltas, counter, csvwriter):
    """ Write empty rows between the two timestamps """

    print("Gap detected? %s -> %s" % (from_timestamp, to_timestamp))
    current_timestamp = from_timestamp
    while current_timestamp < to_timestamp:
        csvwriter.writerow((current_timestamp, None, None))
        current_timestamp = current_timestamp + deltas[counter % len(deltas)]
        counter += 1


# First day (to create measurements for
start_timestamp = datetime(1961, 1, 1)
# Last day (or rather the first day after the measurements)
end_timestamp = datetime(2014, 1, 1)

# Tell the csv-library how the datafiles looks
csv.register_dialect('mikaeldata', delimiter=';', quoting=csv.QUOTE_MINIMAL)

# For all filenames given on the commandline
for file_name in argv[1:]:
    print("%s\n" % file_name)

    # This counter is used to know where in the pattern we are
    row_counter = 0

    with open(file_name) as csvfile:
        # Create a csv reader which reads from the opened file
        headers = None

        csvreader = csv.reader(csvfile, 'mikaeldata')
        for row in csvreader:
            if row and "Datum" == row[0]:
                headers = row
                break

        # Read datapoints from the first 24 hours
        initial_rows = []
        initial_rows.append(csvreader.next())
        initial_rows.append(csvreader.next())

        initial_timestamps = [datetime_from_fields(r) for r in initial_rows]

        while initial_timestamps[0].date() == initial_timestamps[-1].date():
            row = csvreader.next()
            initial_rows.append(row)
            initial_timestamps.append(datetime_from_fields(row))

        # This is sort of magic, it creates a list with the difference between the
        # timestamps during the first day (google for list comprehension if you
        # want to learn about [bla(A) for A in list_of_As])
        # This pattern is then used as a template for all measurements (changing
        # time of measurement during the dataset will break this program)
        delta_pattern = [b - a for a,b in zip(initial_timestamps[0:-1],initial_timestamps[1:])]

        """ Iterate over the timedeltas at the same time as the datarows. As
            long as every day follows the same pattern it should all work out in
            the end.
        """
        
        # Try to handle the different start time for different files
        if initial_timestamps[0].time() == time(6, 0):
            print("First timestamp is at 06:00")
            current_timestamp = start_timestamp + timedelta(hours=6)
        else:
            current_timestamp = start_timestamp

        print("Deltas")
        for d,t in zip(delta_pattern, initial_timestamps):
            print("   %s\t%s" % (d,t))

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

            # Write rows until the first measurement
            write_pad_rows_until_date(current_timestamp, initial_timestamps[0],
                                      delta_pattern, row_counter, csvwriter)

            # Write the previously read rows
            for row in initial_rows:
                current_timestamp = datetime_from_fields(row)
                csvwriter.writerow((current_timestamp, float(row[2]), row[3]))
                row_counter += 1

            # Write the rest of the rows
            for row in csvreader:
                current_timestamp = current_timestamp + delta_pattern[row_counter % len(delta_pattern)]
                row_timestamp = datetime_from_fields(row)
                value = float(row[2])
                quality = row[3]
                # Do we have a gap?
                if current_timestamp < row_timestamp:
                    write_pad_rows_until_date(current_timestamp, row_timestamp,
                                            delta_pattern, row_counter, csvwriter)
                    current_timestamp = row_timestamp

                csvwriter.writerow((row_timestamp, value, quality))

                row_counter += 1

            write_pad_rows_until_date(current_timestamp, end_timestamp,
                                      delta_pattern, row_counter, csvwriter)
