from StringIO import StringIO
import requests
from zipfile import ZipFile
import shutil
import tempfile
import filecmp
import datetime
import os

from basecommand import BaseCommand

"""Practice and CCG metadata, keyed by code.

Similar data, pertaining to specific points in time, is also found in
the files downloaded to `data/raw_data/T<datestamp>ADDR+BNFT.CSV`.

We prefer data from these files to the `ADDR+BNFT` versions, but the
data we download here is only available as current data; this means we
would lack address information for historic data points if we only
relied on these org files.

This data is therefore worth updating every month.

"""


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--ccg', action='store_true')
        parser.add_argument('--practice', action='store_true')
        parser.add_argument('--postcode', action='store_true')

    def handle(self):
        self.url_template = (
            "http://systems.digital.nhs.uk/"
            "data/ods/datadownloads/data-files/%s.zip"
        )
        if self.args.practice:
            self.fetch_practice_details()
        if self.args.ccg:
            self.fetch_ccg_details()
        if self.args.postcode:
            self.fetch_org_postcodes()

    def fetch_ccg_details(self):
        self.fetch_and_extract_zipped_csv(
            self.url_template % 'eccg',
            "data/ccg_details")

    def fetch_practice_details(self):
        self.fetch_and_extract_zipped_csv(
            self.url_template % 'epraccur',
            "data/practice_details")

    def fetch_org_postcodes(self):
        url = (
            "http://systems.digital.nhs.uk/"
            "data/ods/datadownloads/onsdata/zip-files/gridall.zip"
        )
        self.fetch_and_extract_zipped_csv(
            url,
            'data/nhs_postcode_file')

    def fetch_and_extract_zipped_csv(self, url, dest):
        """Grab a zipfile from a url, and extract a CSV.

        Save it to a datestamped folder if it's different from the
        latest previously-known data

        """
        t = tempfile.mkdtemp()[1]
        name = url.split('/')[-1].split('.')[0]
        expected_filename = "%s.csv" % name
        f = StringIO()
        f.write(requests.get(url).content)
        f.flush()
        zipfile = ZipFile(f)
        zipfile.extract(expected_filename, t)
        extracted_file_path = "%s/%s" % (t, expected_filename)
        most_recent = self.most_recent_file(dest)
        changed = not filecmp.cmp(most_recent, extracted_file_path, shallow=True)
        if changed:
            new_folder = datetime.datetime.today().strftime("%Y_%m")
            new_path = "%s/%s/" % (dest, new_folder)
            os.makedirs(new_path)
            if self.args.verbose:
                print "%s has changed; creating new copy" % most_recent
            shutil.copy(extracted_file_path, new_path)
        shutil.rmtree(t)

if __name__ == '__main__':
    Command().handle()
