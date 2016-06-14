import json
import subprocess
import shlex
import networkx as nx
import re
import glob
import datetime
import UserDict
import textwrap
import argparse
import pipes
import time
import os

from googleapiclient import discovery, errors
from googleapiclient.http import MediaFileUpload
from oauth2client.client import GoogleCredentials

from utils.cloud import CloudHandler

OPENP_PYTHON = os.environ['OPENP_PYTHON']
OPENP_DATA_PYTHON = os.environ['OPENP_DATA_PYTHON']
OPENP_DATA_BASEDIR = os.environ['OPENP_DATA_BASEDIR']
OPENP_FRONTEND_APP_BASEDIR = os.environ['OPENP_FRONTEND_APP_BASEDIR']

FILENAME_FLAGS = [
    'filename', 'ccg', 'epraccur', 'chem_file', 'hscic_address']
# Number of bytes to send/receive in each request.
CHUNKSIZE = 2 * 1024 * 1024
DEFAULT_MIMETYPE = 'application/octet-stream'


class NothingToDoError(StandardError):
    pass


class ManifestError(StandardError):
    pass


class Source(UserDict.UserDict):
    """Adds business logic to a row of data in `manifest.json`
    """
    def __init__(self, source):
        UserDict.UserDict.__init__(self)
        self.data = source

    def last_imported_filename(self, file_regex):
        """Return the full path to the most recently imported data for this
        source.

        Returns None if no data has been imported.

        """
        with open('log.json', 'r') as f:
            try:
                log = json.load(f)
            except ValueError:
                log = {}
            dates = log.get(self['id'], [])
            if dates:
                matches = filter(
                    lambda x: re.findall(file_regex, x['imported_file']),
                    dates)
                if matches:
                    return sorted(
                        matches,
                        key=lambda x: x['imported_at'])[-1]['imported_file']

    def set_last_imported_filename(self, filename):
        """Set the path of the most recently imported data for this source
        """
        now = datetime.datetime.now().replace(microsecond=0).isoformat()
        with open('log.json', 'r+') as f:
            try:
                log = json.load(f)
            except ValueError:
                log = {}
            if not log.get(self['id'], None):
                log[self['id']] = []
            log[self['id']].append(
                {'imported_file': filename,
                 'imported_at': now})
            f.seek(0)
            f.write(json.dumps(log, indent=2, separators=(',', ': ')))

    def filename_arg(self, cmd_string):
        """Extract the argument supplied to `--filename` flag (or similar).

        Possible flags indicating a filename are defined in the
        FILENAME_FLAGS constant.

        """
        # We quote before splitting, to preserve regex backslashes
        # specified in the JSON
        cmd_parts = shlex.split(cmd_string.encode('unicode-escape'))
        filename_idx = None
        for flag in FILENAME_FLAGS:
            try:
                filename_idx = cmd_parts.index("--%s" % flag) + 1
            except ValueError:
                pass
        if not filename_idx:
            raise ManifestError(
                "Couldn't find a filename argument in %s"
                % cmd_string)
        return cmd_parts[filename_idx]

    def most_recent_file(self, importer, raise_if_imported=True):
        """Return the most recently generated data file for the specified
        importer.

        If `raise_if_imported` is True, raise a `NothingToDoError` if
        that file has been recorded as already imported.

        """
        if importer:
            file_regex = self.filename_arg(importer)
        else:
            file_regex = '.*'
        files = glob.glob("%s/%s/*/*" % (OPENP_DATA_BASEDIR, self['id']))
        candidates = filter(
            lambda x: re.findall(file_regex, x),
            files)
        if len(candidates) == 0:
            raise StandardError(
                "Couldn't find a file matching %s at %s/%s" %
                (file_regex, OPENP_DATA_BASEDIR, self['id']))
        most_recent = sorted(candidates)[-1]
        last_imported_file = self.last_imported_filename(file_regex)
        if raise_if_imported and last_imported_file:
            last_imported_date = last_imported_file.split("/")[-2]
            most_recent_date = most_recent.split("/")[-2]
            if last_imported_date >= most_recent_date:
                raise NothingToDoError()
        return most_recent

    def importer_cmds_with_latest_data(self):
        """Return a list of importer commands suitable for running.

        This replaces regex-based placeholders defined in the
        `importers` with the full path to the most recent unimported
        data, and omits commands which have already been run for the
        most recent unimported data.

        """
        cmds = []
        for importer in self.get('importers', []):
            try:
                latest_data = self.most_recent_file(importer)
                filename_regex = self.filename_arg(importer)
                cmds.append(importer.replace(
                    filename_regex, pipes.quote(latest_data)))
            except NothingToDoError:
                print "Skipping processing %s" % self['id']
                latest_data = self.most_recent_file(
                    importer, raise_if_imported=False)
                print "    Last processed: %s" % \
                    latest_data.replace(OPENP_DATA_BASEDIR, '')
        return cmds


class ManifestReader(object):
    def __init__(self):
        super(ManifestReader, self).__init__()
        with open('manifest.json') as f:
            self.sources = map(lambda x: Source(x), json.load(f))
            self.sources_with_fetchers = filter(
                lambda x: 'fetcher' in x, self.sources)
            self.sources_without_fetchers = filter(
                lambda x: 'fetcher' not in x, self.sources)

    def source_by_id(self, key):
        return next(x for x in self.sources
                    if x['id'] == key)

    def sources_ordered_by_dependency(self):
        """Produce a list of sources, ordered by dependency graph
        """
        graph = nx.DiGraph()
        for source in self.sources:
            graph.add_node(source['id'])
            for parent in source.get('depends_on', []):
                graph.add_node(parent)
                graph.add_edge(parent, source['id'])
        resolved_order = nx.topological_sort(graph)
        return sorted(
            self.sources,
            key=lambda s: resolved_order.index(s['id']))


class BigQueryUploader(ManifestReader, CloudHandler):
    def upload_all_to_storage(self):
        bucket = 'ebmdatalab'
        for source in self.sources:
            for importer in source.get('importers', []):
                path = source.most_recent_file(
                    importer, raise_if_imported=False)
                name = 'hscic' + path.replace(OPENP_DATA_BASEDIR, '')
                if self.dataset_exists(bucket, name):
                    print "Skipping %s, already uploaded" % name
                print "Uploading %s to %s" % (path, name)
                self.upload(path, bucket, name)

    def upload_prescribing_to_storage(self):
        prescribing = self.source_by_id('prescribing')
        bucket = 'ebmdatalab'
        for importer in prescribing['importers']:
            path = prescribing.most_recent_file(
                importer, raise_if_imported=False)
            object_name = os.path.split(path)[-1]
            if 'CHEM' in path:
                object_prefix = 'hscic/chemicals/%s'
            elif 'ADDR' in path:
                object_prefix = 'hscic/addresses/%s'
            elif 'BNFT' in path:
                object_prefix = 'hscic/prescribing/%s'
            self.upload(path, bucket, object_prefix % object_name)

    def _count_imported_data_for_filename(self, filename,
                                          table_name='prescribing'):
        """Given a CSV filename for prescribing data, query how many rows have
        already been ingested for that date in the main `prescribing`
        table.

        """
        match = re.match(r'.*T(\d{6})PDPI', filename)
        date = match.groups()[0]
        year = date[:4]
        month = date[4:]
        query = ('SELECT count(*) AS count FROM [hscic.%s] '
                 'WHERE month = "%s-%s-01 00:00:00"' %
                 (table_name, year, month))
        try:
            response = self.bigquery.jobs().query(
                projectId='ebmdatalab',
                body={'query': query}).execute()
            return int(response['rows'][0]['f'][0]['v'])
        except errors.HttpError as e:
            if e.resp.status == 404:
                # The table doesn't exist; not an error as we will create it
                return 0
            else:
                raise

    def update_prescribing_table(self):
        """Update `prescribing` table from cloud-stored CSV
        """
        dest_table = 'prescribing_example'
        most_recent = self.list_raw_datasets(
            'ebmdatalab', 'hscic/prescribing')[-1]
        count = self._count_imported_data_for_filename(
            most_recent, table_name=dest_table)
        if count > 0:
            msg = "There are already %s rows for %s imported"
            raise StandardError(msg % (count, most_recent))
        uri = "gs://ebmdatalab/%s" % most_recent
        query = ('SELECT sha, pct, practice, bnf_code, bnf_name, items, '
                 'net_cost, actual_cost, quantity, '
                 'TIMESTAMP(period + "01") AS month '
                 'FROM [hscic.prescribing_temp] LIMIT 10')
        print "Loading data from %s to temporary table..." % uri
        self.load(uri, table_name="prescribing_temp")
        print "Querying temporary table and appending to %s..." % dest_table
        self.query_and_save(
            query,
            dest_table=dest_table,
            mode='append')


class FetcherRunner(ManifestReader):
    def prompt_all_manual_data(self):
        """Emit a sequence of easy-to-follow instructions for freshing data.

        A sticking plaster for the fact that not all sources can be
        fetched automatically.

        """
        month_and_day = datetime.datetime.now().\
            strftime('%Y_%m')
        for source in self.sources_without_fetchers:
            if 'core_data' not in source['tags']:
                continue
            for importer in source.get('importers', [None]):
                expected_location = "%s/%s/%s" % (
                    OPENP_DATA_BASEDIR, source['id'], month_and_day)
                print
                print "Locate latest data for %s, if available" % source['id']
                print "Save it at:"
                print "    %s" % expected_location
                if 'index_url' in source and source['index_url']:
                    print "Where to look:"
                    print "   %s" % source['index_url']
                if 'urls' in source:
                    print "Previous data has been found at at:"
                    for k, v in source['urls'].items():
                        print "    %s: %s" % (k, v)
                if source.get('publication_schedule', None):
                    print "Publication frequency:"
                    print "    %s" % source['publication_schedule']
                if 'notes' in source:
                    print "Notes:"
                    for line in textwrap.wrap(source['notes']):
                        print "    %s" % line
                print "The last saved data can be found at:"
                print "    %s" % \
                    source.most_recent_file(importer, raise_if_imported=False)
                raw_input("Press return when done")

    def run_all_fetchers(self):
        """Run every fetcher defined in the manifest
        """
        for source in self.sources_with_fetchers:
            command = "%s fetchers/%s" % (OPENP_DATA_PYTHON, source['fetcher'])
            print "Running %s" % command

            subprocess.check_call(shlex.split(command))


class ImporterRunner(ManifestReader):
    def update_log(self):
        """Update the log to indicate all importers to date have been run.

        Useful if you've imported stuff manually and don't want the
        importers to be run again.

        """
        for source in self.sources_ordered_by_dependency():
            for importer in source.get('importers', []):
                most_recent = source.most_recent_file(
                    importer, raise_if_imported=False)
                source.set_last_imported_filename(most_recent)

    def run_all_importers(self, paranoid=False):
        """Run each importer sequentially.

        On success, logs each one as imported
        """
        for source in self.sources_ordered_by_dependency():
            for cmd in source.importer_cmds_with_latest_data():
                print "Importing %s with command: `%s`" % (
                    source['id'], cmd)
                if paranoid:
                    if raw_input("Continue? [y/n]").lower() != 'y':
                        print "  Skipping...."
                        continue
                run_cmd = run_management_command(cmd)
                input_file = source.filename_arg(run_cmd)
                source.set_last_imported_filename(input_file)
            if 'after_import' in source:
                run_management_command(source['after_import'])


def run_management_command(cmd):
    """Run a Django management command.

    Raise an exception if the command is not successful
    """
    cmd_to_run = "%s %s/manage.py %s" % (
        OPENP_PYTHON, OPENP_FRONTEND_APP_BASEDIR, cmd)
    p = subprocess.Popen(
        shlex.split(cmd_to_run),
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        cwd=OPENP_FRONTEND_APP_BASEDIR
    )
    stdout, stderr = p.communicate()
    print stdout
    if p.returncode:
        error = "Problem when running %s\n" % cmd
        error += stdout + "\n"
        error += stderr
        raise StandardError(error)
    return cmd_to_run


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        'command', metavar='COMMAND',
        nargs=1,
        type=str,
        choices=['getmanual', 'getauto', 'updatelog',
                 'runimporters', 'bigquery', 'create_indexes',
                 'create_matviews', 'refresh_matviews',
                 'archivedata']

    )
    parser.add_argument('--bigquery-file')
    parser.add_argument('--paranoid', action='store_true')
    args = parser.parse_args()
    if args.command[0] == 'getmanual':
        FetcherRunner().prompt_all_manual_data()
    elif args.command[0] == 'getauto':
        FetcherRunner().run_all_fetchers()
    elif args.command[0] == 'runimporters':
        ImporterRunner().run_all_importers(paranoid=args.paranoid)
    elif args.command[0] == 'updatelog':
        ImporterRunner().update_log()
    elif args.command[0] == 'updatebigquery':
        BigQueryUploader().update_prescribing_table()
    elif args.command[0] == 'uploaddata':
        BigQueryUploader().upload_all_to_storage()
    elif args.command[0] == 'create_indexes':
        run_management_command('create_indexes')
    elif args.command[0] == 'create_matviews':
        run_management_command('create_matviews')
    elif args.command[0] == 'refresh_matviews':
        run_management_command('refresh_matviews')
