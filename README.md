**This repository is retired and no longer used. All OpenPrescribing data management is carried out by the `pipeline` app in [the main OpenPrescribing repo](https://github.com/ebmdatalab/openprescribing/)**

# Overview

## Regular updating

You should probably do this in a `screen` session on the production
server, as the `refresh_matviews` part, in particular, takes very many
hours.  To set up a new session:

    ssh hello@openp
    screen -S monthly-data-update
    cd ~/openprescribing-data/
    source .venv/bin/activate

If your environment is set up (see below), you can do the following to update the monthly data:

    cp log.json log.json-2016-06-01    # backup the log file

    python runner.py getmanual         # manually source some of the data:
    python runner.py getauto           # automatically source the rest
    python runner.py archivedata       # store all most recent data in Google Cloud storage
    python runner.py runimporters      # import any previously unimported data
    python runner.py updatesmoketests  # update smoke tests
    python runner.py runsmoketests     # store latest prescribing data to BQ (requires `archivedata` to have been run)
    git commit -am "Update smoketests"

To see data in production, you should purge the Cloudflare cache. To
do this, go to your openprescribing sandbox and run:

    fabric clear_cloudflare:purge_all

## Set up a dev sandbox

    touch log.json
    python runner.py getdata           # grab latest version of data from Google Cloud
    python runner.py runimporters      # import any previously unimported data
    python runner.py create_indexes    # indexes in postgres DB
    python runner.py create_matviews   # materialized views in DB. Takes ages.


Among other things, this will import the latest month of prescribing data. The total set of prescribing data is massive. To avoid locking your computer up for a week, you can generate a random subset from the production database thus:

    prescribing=> CREATE TABLE sample_prescription AS SELECT * FROM (SELECT DISTINCT 1 + trunc(random() * 795362276)::integer AS id FROM generate_series(1, 10100) g) r JOIN frontend_prescription USING (id) LIMIT 10000;


    $ pg_dump --table=sample_prescription --data-only --column-inserts prescribing --user prescribing > /tmp/data.sql

Then import it to your local database by replacing `sample_prescription` with `frontend_prescription` in the SQL file, and importing it with `psql prescribing < /tmp/data.sql`.

# Setup

Install python dependencies:

    pip install -r requirements.txt

Set up environment variables:

    OPENP_PYTHON=/webapps/openprescribing/.venv/bin/python"
    OPENP_FRONTEND_APP_BASEDIR=/webapps/openprescribing/openprescribing
    OPENP_DATA_PYTHON=/home/hello/openprescribing-data/.venv/bin/python"
    OPENP_DATA_BASEDIR=/home/hello/openprescribing-data/data

For bigquery-loading support, add the following variable to your environment:

    GOOGLE_APPLICATION_CREDENTIALS=<path-to-credentials.json>

...where `credentials.json` is the output of

   pass show google-service-accounts/bigquery



# How it works

`manifest.json` contains a list of sources that we use in
OpenPrescribing, with descriptions and metadata.

**id** *(required)*: a unique id for this source

**title** *(required)*: short title for the source

**description** *(required)*: description of the source, including important features, rationale for including it in OpenPrescribing, etc

**fetcher**: The name of a python script, which should be placed in the `fetchers/` directory, which gets data for this source. Fetchers should be idempotent. When run, if a fetcher finds new data, it should place the new data in a timestamped folder at `data/<id>/<year>_<month>`.

**importers**: a list of importers, each element of which should be the name of a Django management command (plus switches) in the main app which knows how to import this data. The command must have a `--filename` switch, and the `importer` definition must include a regex as its value which is expected to match the filename

**after_import**: a list of Django management commands that should be run following a successful import run.

**depends_on**: a list of source ids which should be imported before this source can be imported.

**index_url**: a webpage where the latest version of the dataset can be found by a user

**urls**: A dictionary of URLs where previous data has been found. This is informational, to help a user hunt down the latest version

**tags** *(required)*: a list of tags. Only sources tagged `core_data` are considered manual sources (see below). Otherwise tags are currently just informational

**publication_schedule**: a human-readable string giving the expected publication schedule

**publication_lag**: a human-readable string describing how long after the reporting date the dataset is published

A source without `fetchers`, and with the `core_data` tag, is deemed a
manual source, and therefore appears in the prompt list generated by
`python runner.py getmanual`.

Finally, all the raw data is stored in Google BigQuery.

# TODO

* Better argument parsing (using subcommands)
* Refactor `runner.py` for readability
* Tests
