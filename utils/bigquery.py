from gcloud.bigquery import SchemaField
import psycopg2
from gcloud import bigquery
import time
import csv
import tempfile
from os import environ
from django.core.exceptions import ImproperlyConfigured


PRESCRIBING_SCHEMA = [
    SchemaField('sha', 'STRING'),
    SchemaField('pct', 'STRING'),
    SchemaField('practice', 'STRING'),
    SchemaField('bnf_code', 'STRING'),
    SchemaField('bnf_name', 'STRING'),
    SchemaField('items', 'INTEGER'),
    SchemaField('net_cost', 'FLOAT'),
    SchemaField('actual_cost', 'FLOAT'),
    SchemaField('quantity', 'INTEGER'),
    SchemaField('month', 'TIMESTAMP'),
]

PRACTICE_SCHEMA = [
   SchemaField('code', 'STRING'),
   SchemaField('name', 'STRING'),
   SchemaField('address1', 'STRING'),
   SchemaField('address2', 'STRING'),
   SchemaField('address3', 'STRING'),
   SchemaField('address4', 'STRING'),
   SchemaField('address5', 'STRING'),
   SchemaField('postcode', 'STRING'),
   SchemaField('location', 'STRING'),
   SchemaField('area_team_id', 'STRING'),
   SchemaField('ccg_id', 'STRING'),
   SchemaField('setting', 'INTEGER'),
   SchemaField('close_date', 'STRING'),
   SchemaField('join_provider_date', 'STRING'),
   SchemaField('leave_provider_date', 'STRING'),
   SchemaField('open_date', 'STRING'),
   SchemaField('status_code', 'STRING'),
 ]

PRACTICE_STATISTICS_SCHEMA = [
    SchemaField('date', 'TIMESTAMP'),
    SchemaField('male_0_4', 'INTEGER'),
    SchemaField('female_0_4', 'INTEGER'),
    SchemaField('male_5_14', 'INTEGER'),
    SchemaField('male_15_24', 'INTEGER'),
    SchemaField('male_25_34', 'INTEGER'),
    SchemaField('male_35_44', 'INTEGER'),
    SchemaField('male_45_54', 'INTEGER'),
    SchemaField('male_55_64', 'INTEGER'),
    SchemaField('male_65_74', 'INTEGER'),
    SchemaField('male_75_plus', 'INTEGER'),
    SchemaField('female_5_14', 'INTEGER'),
    SchemaField('female_15_24', 'INTEGER'),
    SchemaField('female_25_34', 'INTEGER'),
    SchemaField('female_35_44', 'INTEGER'),
    SchemaField('female_45_54', 'INTEGER'),
    SchemaField('female_55_64', 'INTEGER'),
    SchemaField('female_65_74', 'INTEGER'),
    SchemaField('female_75_plus', 'INTEGER'),
    SchemaField('total_list_size', 'INTEGER'),
    SchemaField('astro_pu_cost', 'FLOAT'),
    SchemaField('astro_pu_items', 'FLOAT'),
    SchemaField('star_pu', 'STRING'),
    SchemaField('pct_id', 'STRING'),
    SchemaField('practice_id', 'STRING')
]


def get_env_setting(setting, default=None):
    """ Get the environment setting.

    Return the default, or raise an exception if none supplied
    """
    try:
        return environ[setting]
    except KeyError:
        if default:
            return default
        else:
            error_msg = "Set the %s env variable" % setting
            raise ImproperlyConfigured(error_msg)


def load_data_from_file(
        dataset_name, table_name,
        source_file_name, schema, _transform=None):
    client = bigquery.Client(project='ebmdatalab')
    dataset = client.dataset(dataset_name)
    table = dataset.table(
        table_name,
        schema=schema)
    if not table.exists():
        table.create()
    table.reload()
    with tempfile.TemporaryFile(mode='rb+') as csv_file:
        with open(source_file_name, 'rb') as source_file:
            writer = csv.writer(csv_file)
            reader = csv.reader(source_file)
            for row in reader:
                if _transform:
                    row = _transform(row)
                writer.writerow(row)
        job = table.upload_from_file(
            csv_file, source_format='text/csv',
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            rewind=True)
        wait_for_job(job)


def load_prescribing_data_from_file(
        dataset_name, table_name, source_file_name):
    def _transform(row):
        # To match the prescribing table format in BigQuery, we have
        # to re-encode the date field as a bigquery TIMESTAMP and drop
        # a couple of columns
        row[10] = "%s 00:00:00" % row[10]
        del(row[3])
        del(row[-1])
        return row
    return load_data_from_file(
        dataset_name, table_name,
        source_file_name, PRESCRIBING_SCHEMA, _transform=_transform)


def load_data_from_pg(dataset_name, bq_table_name, pg_table_name, schema):
    """Loads every row currently in the `frontend_practice` table to the
    specified table in BigQuery

    """
    db_name = get_env_setting('DB_NAME')
    db_user = get_env_setting('DB_USER')
    db_pass = get_env_setting('DB_PASS')
    db_host = get_env_setting('DB_HOST', '127.0.0.1')
    conn = psycopg2.connect(database=db_name, user=db_user,
                            password=db_pass, host=db_host)
    with tempfile.NamedTemporaryFile(mode='r+b') as csv_file:
        cols = [x.name for x in schema]
        conn.cursor().copy_to(
            csv_file, pg_table_name, sep=',', null='', columns=cols)
        csv_file.seek(0)

        load_data_from_file(
            dataset_name, bq_table_name,
            csv_file.name,
            schema
        )
        conn.commit()
        conn.close()


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.error_result)
            return
        time.sleep(1)
