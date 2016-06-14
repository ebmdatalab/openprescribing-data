# Overview

## Regular updating

    python runner.py getmanual         # manually source some of the data
    python runner.py getauto           # automatically source the rest
    python runner.py runimporters      # import any previously unimported data
    python runner.py refresh_matviews  # materialized views in DB
    python runner.py uploaddata        # store all most recent data in Google Cloud storage
    python runner.py updatebigquery    # store latest prescribing data to BQ (requires `uploaddata` to have been run)

To see data in production, you should purge the Cloudflare cache. To
do this, go to your openprescribing sandbox and run:

    fabric clear_cloudflare:purge_all

## First run (e.g. to set up dev sandbox)

    python runner.py getauto           # grab latest version of automated data
    python runner.py runimporters      # import any previously unimported data
    python runner.py create_indexes    # indexes in postgres DB
    python runner.py create_matviews   # materialized views in DB


## Smoke tests


Also:

    python manage.py refresh_matviews -v 2

And finally, update the smoke tests in `smoke.py` - you'll need to update `NUM_RESULTS`, plus add expected values for the latest month.

Then re-run the smoke tests against the live data to make sure everything looks as you expect:

    python smoketests/smoke.py
i
Purge Cloudflare cache with fabric


smoketests
Run:
 python smoketests/smoke.py

Generate data:
* run smoke.sh
* update smoke.py

    NUM_RESULTS = 66  # Should equal number of months since Aug 2010.
    NUM_RESULTS_CCG = 34 # Should equal number of months since Apr 2013.

** I need the queries off Anna **


# Setup

Install python dependencies:

    pip install -r requirements.txt

Set up environment variables:

    OPENP_PYTHON=/webapps/openprescribing/.venv/bin/python"
    OPENP_FRONTEND_APP_DIR=/webapps/openprescribing/openprescribing
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

# Outstanding questions

## Importing measures

In the OpenPrescribing README, it says

> Then update measures. You can get the raw list size data from the
> [BSA Information Portal](https://apps.nhsbsa.nhs.uk/infosystems/welcome)
> (Report > Common Information Reports > Organisation Data > Practice
> List Size):
>
>    python manage.py import_measures --start_date YYYY-MM-DD --end_date YYYY-MM-DD -v 2

I don't understand this. Which raw list size data? How does
import_measures depend on this?

Once I understand this, I can add an importer for this step to the
appropriate data source.

## BigQuery import

Whichever source file I attempt to load to BigQuery (using a sample of just a few lines of data) I get errors like:

> Too many values in row starting at position:0 in file:file-0000000

I presume we should be importing a specific, massaged version of the
file. Where is it / how is it generated?

## General understanding of the data

I've tried to write up what I think all the data is for clearly in
`manifest.json`. However, I'm sure I've made mistakes. I've also tried
to highlight specific queries in the `notes` field. It would be good
to go over this together.

In particular, a number of sources appear to update monthly, but we
don't update them, and in some of these cases the importers appear not
to work (e.g. the CCG boundaries in KML format).

# TODO

* Better argument parsing (using subcommands)
* Refactor `runner.py` for readability
* Tests?
