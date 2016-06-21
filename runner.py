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
import os
import errno

from utils.cloud import CloudHandler

OPENP_PYTHON = os.environ['OPENP_PYTHON']
OPENP_DATA_PYTHON = os.environ['OPENP_DATA_PYTHON']
OPENP_DATA_BASEDIR = os.environ['OPENP_DATA_BASEDIR']
OPENP_FRONTEND_APP_BASEDIR = os.environ['OPENP_FRONTEND_APP_BASEDIR']

FILENAME_FLAGS = [
    'filename', 'ccg', 'epraccur', 'chem_file', 'hscic_address',
    'month_from_prescribing_filename']
# Number of bytes to send/receive in each request.
CHUNKSIZE = 2 * 1024 * 1024
DEFAULT_MIMETYPE = 'application/octet-stream'


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class NothingToDoError(StandardError):
    pass


class ManifestError(StandardError):
    pass


class FileNotFoundError(StandardError):
    pass


class Source(UserDict.UserDict):
    """Adds business logic to a row of data in `manifest.json`
    """
    def __init__(self, source):
        UserDict.UserDict.__init__(self)
        self.data = source

    def last_imported_file(self, file_regex):
        """Return the full path to the most recently imported data for this
        source.

        Returns None if no data has been imported.

        """
        with open('log.json', 'r') as f:
            log = json.load(f)
            dates = log.get(self['id'], [])
            if dates:
                matches = filter(
                    lambda x: re.findall(file_regex, x['imported_file']),
                    dates)
                if matches:
                    return sorted(
                        matches,
                        key=lambda x: x['imported_at'])[-1]

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

    def files_by_date(self, importer):
        if importer:
            file_regex = self.filename_arg(importer)
        else:
            file_regex = '.*'
        data_location = os.path.join(
            OPENP_DATA_BASEDIR, self.get('data_dir', self['id']))
        files = glob.glob("%s/*/*" % data_location)
        candidates = filter(
            lambda x: re.findall(file_regex, x),
            files)
        if len(candidates) == 0:
            raise FileNotFoundError(
                "Couldn't find a file matching %s at %s/%s" %
                (file_regex, OPENP_DATA_BASEDIR, data_location))
        return sorted(candidates)

    def unimported_files(self, importer):
        """Return a list of files that have not been imported for a given
        importer.

        """
        if importer:
            file_regex = self.filename_arg(importer)
        else:
            file_regex = '.*'
        assume_imported_to = self.last_imported_file(file_regex)
        if assume_imported_to:
            last_imported_date = \
              assume_imported_to['imported_file'].split("/")[-2]
        else:
            last_imported_date = ""
        try:
            all_files = self.files_by_date(importer)
            return filter(
                lambda x: x.split('/')[-2] < last_imported_date,
                all_files)
        except FileNotFoundError:
            return []

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
        most_recent = self.unimported_files()[-1]
        last_imported_file = self.last_imported_file(file_regex)
        if raise_if_imported and last_imported_file:
            last_imported_date = last_imported_file['imported_file'].split("/")[-2]
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
            for latest_data in self.unimported_files(importer):
                filename_regex = self.filename_arg(importer)
                cmds.append(importer.replace(
                    filename_regex, pipes.quote(latest_data)))
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

class SmokeTestHandler(ManifestReader, CloudHandler):
    def run_smoketests(self):
        prescribing = self.source_by_id('prescribing')
        last_imported = prescribing.last_imported_file(
            r'_formatted\.CSV$')['imported_file']
        date = re.findall(r'/(\d{4}_\d{2})/', last_imported)[0]
        command = "%s smoketests/smoke.py" % OPENP_DATA_PYTHON
        my_env = os.environ.copy()
        my_env['LAST_IMPORTED'] = date
        print "Running %s with LAST_IMPORTED=%s" % (command, date)
        subprocess.check_call(shlex.split(command), env=my_env)

    def update_smoketests(self):
        for sql_file in glob.glob('smoketests/*sql'):
            test_name = os.path.splitext(
                os.path.basename(sql_file))[0]
            with open(sql_file, 'rb') as f:
                query = f.read()
                response = self.bigquery.jobs().query(
                    projectId='ebmdatalab',
                    body={'useLegacySql': False, 'timeoutMs': 20000, 'query': query}).execute()
                quantity = []
                cost = []
                items = []
                for r in self.rows_to_dict(response):
                    quantity.append(r['quantity'])
                    cost.append(r['actual_cost'])
                    items.append(r['items'])
                print "Updating test expectations for %s" % test_name
                with open("%s.json" % test_name, 'wb') as f:
                    obj = {'cost': cost,
                           'items': items,
                           'quantity': quantity}
                    json.dump(obj, f, indent=2)


class BigQueryDownloader(ManifestReader, CloudHandler):
    def download_all(self):
        bucket = 'ebmdatalab'
        for source in self.sources:
            base_name = 'hscic/%s' % source['id']
            for importer in source.get('importers', []):
                filename_regex = source.filename_arg(importer)
                try:
                    most_recent = self.list_raw_datasets(
                        bucket, prefix=base_name,
                        name_regex=filename_regex)[-1]
                except IndexError:
                    print "No file in Cloud at %s matching %s" % (
                        base_name, filename_regex)
                    continue
                target_file = os.path.join(
                    OPENP_DATA_BASEDIR,
                    most_recent.replace('hscic/', ''))
                target_dir = os.path.split(target_file)[0]
                if os.path.exists(target_file):
                    print "%s exists; skipping" % target_file
                else:
                    print "Downloading %s to %s" % (most_recent, target_file)
                    mkdir_p(target_dir)
                    self.download(target_file, bucket, most_recent)


class BigQueryUploader(ManifestReader, CloudHandler):
    def upload_all_to_storage(self):
        bucket = 'ebmdatalab'
        for source in self.sources:
            for importer in source.get('importers', []):
                try:
                    for path in source.files_by_date(importer):
                        name = 'hscic' + path.replace(OPENP_DATA_BASEDIR, '')
                        if self.dataset_exists(bucket, name):
                            print "Skipping %s, already uploaded" % name
                        print "Uploading %s to %s" % (path, name)
                        self.upload(path, bucket, name)
                except FileNotFoundError:
                    pass

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
        months_to_consider = 3
        # build a list of datasets not already dealt with
        raw_datasets = self.list_raw_datasets(
            'ebmdatalab', prefix='hscic/prescribing',
            name_regex=r'PDPI.BNFT\.CSV')
        for dataset in raw_datasets[-months_to_consider:]:
            count = self._count_imported_data_for_filename(
                dataset, table_name=dest_table)
            if count > 0:
                msg = "There are already %s rows for %s imported"
                print msg % (count, dataset)
            else:
                uri = "gs://ebmdatalab/%s" % dataset
                query = ('SELECT sha, pct, practice, bnf_code, bnf_name, '
                         'items, net_cost, actual_cost, quantity, '
                         'TIMESTAMP(period + "01") AS month '
                         'FROM [hscic.prescribing_temp]')
                print "Loading data from %s to temporary table..." % uri
                self.load(uri, table_name="prescribing_temp")
                print ("Querying temporary table and appending to %s..."
                       % dest_table)
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
            # XXX special case prescribing source to run twice. This
            # is because the first time, the formatted version of the
            # file has not been generated. This needs to be fixed to
            # be more elegant, e.g. by moving the conversion to a
            # pre-import step
            for attempt in range(2):
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
                if source['id'] != 'prescribing':
                    break
        if 'after_import' in source:
            run_management_command(source['after_import'])


def run_management_command(cmd):
    """Run a Django management command.

    Raise an exception if the command is not successful
    """
    start = datetime.datetime.now()
    cmd_to_run = "%s %s/manage.py %s -v 2" % (
        OPENP_PYTHON, OPENP_FRONTEND_APP_BASEDIR, cmd)
    now = datetime.datetime.now()
    p = subprocess.Popen(
        shlex.split(cmd_to_run),
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        cwd=OPENP_FRONTEND_APP_BASEDIR
    )
    stdout, stderr = p.communicate()
    print stdout
    print "Command completed in %s seconds" % (datetime.datetime.now() - start).seconds
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
                 'archivedata', 'smoketests', 'updatesmoketests', 'runsmoketests', 'getdata']
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
    elif args.command[0] == 'bigquery':
        BigQueryUploader().update_prescribing_table()
    elif args.command[0] == 'archivedata':
        BigQueryUploader().upload_all_to_storage()
    elif args.command[0] == 'getdata':
        BigQueryDownloader().download_all()
    elif args.command[0] == 'create_indexes':
        run_management_command('create_indexes')
    elif args.command[0] == 'create_matviews':
        run_management_command('create_matviews')
    elif args.command[0] == 'refresh_matviews':
        run_management_command('refresh_matviews')
    elif args.command[0] == 'updatesmoketests':
        SmokeTestHandler().update_smoketests()
    elif args.command[0] == 'runsmoketests':
        SmokeTestHandler().run_smoketests()
