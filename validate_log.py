# Validates that log file only contains logs for sources in manifest, and that
# each log record contains exactly two keys: imported_file and imported_at

import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('--log', help='path to log file', default="log.json")
parser.add_argument('--manifest', help='path to manifest file', default="manifest.json")
args = parser.parse_args()

with open(args.log) as f:
    logs = json.load(f)


with open(args.manifest) as f:
    manifest = json.load(f)


source_ids = [source['id'] for source in manifest]

for source_id, source_logs in logs.items():
    print 'Checking {} is in manifest'.format(source_id)
    assert source_id in source_ids

    print '{} has {} logs'.format(source_id, len(source_logs))

    for log in source_logs:
        assert len(log) == 2
        assert 'imported_file' in log
        assert 'imported_at' in log

print 'All OK'
