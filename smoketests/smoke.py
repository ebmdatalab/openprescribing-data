import csv
import json
import StringIO
import requests
import unittest
import os
from datetime import datetime

'''
Run smoke tests against live site. 35 separate tests to run.
Spending BY: one practice, multiple practices, one CCG,
multiple CCGs, all
Spending ON: one presentation, multiple presentations, one chemical,
multiple chemicals, one section, multiple sections, all
The expected numbers are generated from smoke.sh
'''


class SmokeTestBase(unittest.TestCase):

    DOMAIN = 'https://openprescribing.net'


    def _now_date(self):
        if 'LAST_IMPORTED' in os.environ:
            now = datetime.strptime(os.environ['LAST_IMPORTED'], "%Y_%m")
        else:
            now = datetime.now()
        return now

    def _months_since_data_started(self):
        now = self._now_date()
        return (now.year - 2010) * 12 + (now.month - 8)

    def _months_since_ccg_creation(self):
        now = self._now_date()
        return (now.year - 2013) * 12 + (now.month - 4)

    def _run_tests(self, test, url, expected_total):
        r = requests.get(url)
        f = StringIO.StringIO(r.text)
        reader = csv.DictReader(f)
        all_rows = []
        for row in reader:
            all_rows.append(row)
        self.assertEqual(len(all_rows), expected_total)
        with open("%s.json" % test, 'rb') as f:
            expected = json.load(f)
            for i, row in enumerate(all_rows):
                self.assertEqual(
                    '%.2f' % float(row['actual_cost']), expected['cost'][i])
                self.assertEqual(row['items'], expected['items'][i])
                self.assertEqual(row['quantity'], expected['quantity'][i])


class TestSmokeTestSpendingByEveryone(SmokeTestBase):
    def test_presentation_by_all(self):
        url = '%s/api/1.0/spending/?format=csv&' % self.DOMAIN
        url += 'code=0501013B0AAAAAA'
        self._run_tests('presentation_by_all',
                        url,
                        self._months_since_data_started())

    def test_chemical_by_all(self):
        url = '%s/api/1.0/spending/?format=csv&' % self.DOMAIN
        url += 'code=0407010F0'
        self._run_tests('chemical_by_all',
                        url,
                        self._months_since_data_started())

    def test_bnf_section_by_all(self):
        url = '%s/api/1.0/spending/?format=csv&' % self.DOMAIN
        url += 'code=0702'
        self._run_tests('bnf_section_by_all',
                        url,
                        self._months_since_data_started())


class TestSmokeTestSpendingByOnePractice(SmokeTestBase):
    def test_presentation_by_one_practice(self):
        url = '%s/api/1.0/spending_by_practice/?format=csv&' % self.DOMAIN
        url += 'code=0703021Q0BBAAAA&org=A81015'  # Cerazette 75mcg.
        self._run_tests('presentation_by_one_practice',
                        url,
                        self._months_since_data_started())

    def test_chemical_by_one_practice(self):
        url = '%s/api/1.0/spending_by_practice/?' % self.DOMAIN
        url += 'format=csv&code=0212000AA&org=A81015'  # Rosuvastatin Calcium.
        self._run_tests('chemical_by_one_practice',
                        url,
                        self._months_since_data_started())

    def test_multiple_chemicals_by_one_practice(self):
        url = '%s/api/1.0/spending_by_practice/?format=csv&' % self.DOMAIN
        url += 'code=0212000B0,0212000C0,0212000M0,0212000X0,0212000Y0'
        url += '&org=C85020'  # Multiple generic statins.
        self._run_tests('multiple_chemicals_by_one_practice',
                        url,
                        self._months_since_data_started())

    def test_bnf_section_by_one_practice(self):
        url = '%s/api/1.0/spending_by_practice/' % self.DOMAIN
        url += '?format=csv&code=0304&org=L84077'
        self._run_tests('bnf_section_by_one_practice',
                        url,
                        self._months_since_data_started())


class TestSmokeTestSpendingByCCG(SmokeTestBase):
    def test_presentation_by_one_ccg(self):
        url = '%s/api/1.0/spending_by_ccg?' % self.DOMAIN
        url += 'format=csv&code=0403030E0AAAAAA&org=10Q'
        self._run_tests('presentation_by_one_ccg',
                        url,
                        self._months_since_ccg_creation())

    def test_chemical_by_one_ccg(self):
        url = '%s/api/1.0/spending_by_ccg?' % self.DOMAIN
        url += 'format=csv&code=0212000AA&org=10Q'
        self._run_tests('chemical_by_one_ccg',
                        url,
                        self._months_since_ccg_creation())

    def test_bnf_section_by_one_ccg(self):
        url = '%s/api/1.0/spending_by_ccg?' % self.DOMAIN
        url += 'format=csv&code=0801&org=10Q'
        self._run_tests('bnf_section_by_one_ccg',
                        url,
                        self._months_since_ccg_creation())


class TestSmokeTestMeasures(SmokeTestBase):

    '''
    Smoke tests for all 13 KTTs, for the period July-Sept 2015.
    Cross-reference against data from the BSA site.
    NB BSA calculations are done over a calendar quarter, and ours are done
    monthly, so sometimes we have to multiply things to get the same answers.
    '''

    def get_data_for_q3_2015(self, data):
        total = {
            'numerator': 0,
            'denominator': 0
        }
        for d in data:
            if (d['date'] == '2015-07-01') or \
               (d['date'] == '2015-08-01') or \
               (d['date'] == '2015-09-01'):
                total['numerator'] += d['numerator']
                total['denominator'] += d['denominator']
        total['calc_value'] = (
            total['numerator'] / float(total['denominator'])) * 100
        return total

    def retrieve_data_for_measure(self, measure, practice):
        self.DOMAIN = 'https://openprescribing.net'
        url = '%s/api/1.0/measure_by_practice/?format=json&' % self.DOMAIN
        url += 'measure=%s&org=%s' % (measure, practice)
        r = requests.get(url)
        data = json.loads(r.text)
        rows = data['measures'][0]['data']
        return self.get_data_for_q3_2015(rows)

    def test_measure_by_practice(self):
        q = self.retrieve_data_for_measure(
            'ktt3_lipid_modifying_drugs', 'A81001')
        bsa = {
            'numerator': 34,
            'denominator': 1265,
            'calc_value': '2.688'
        }
        self.assertEqual(q['numerator'], bsa['numerator'])
        self.assertEqual(q['denominator'], bsa['denominator'])
        self.assertEqual("%.3f" % q['calc_value'], bsa['calc_value'])

        q = self.retrieve_data_for_measure(
            'ktt8_antidepressant_first_choice', 'A81001')
        bsa = {
            'numerator': 643,
            'denominator': 1025,
            'calc_value': '62.732'
        }
        self.assertEqual(q['numerator'], bsa['numerator'])
        self.assertEqual(q['denominator'], bsa['denominator'])
        self.assertEqual("%.3f" % q['calc_value'], bsa['calc_value'])

        q = self.retrieve_data_for_measure('ktt8_dosulepin', 'A81001')
        bsa = {
            'numerator': 24,
            'denominator': 1025,
            'calc_value': '2.341'
        }
        self.assertEqual(q['numerator'], bsa['numerator'])
        self.assertEqual(q['denominator'], bsa['denominator'])
        self.assertEqual("%.3f" % q['calc_value'], bsa['calc_value'])

        q = self.retrieve_data_for_measure('ktt9_antibiotics', 'A81001')
        bsa = {
            'numerator': 577,
            'denominator': 7581.92,  # BSA's actual STAR-PU value is 7509
            'calc_value': (577 / 7581.92) * 100
        }
        self.assertEqual(q['numerator'], bsa['numerator'])
        self.assertEqual(
            "%.0f" % q['denominator'], "%.0f" % bsa['denominator'])
        self.assertEqual("%.2f" % q['calc_value'], "%.2f" % bsa['calc_value'])

        q = self.retrieve_data_for_measure('ktt9_cephalosporins', 'A81001')
        bsa = {
            'numerator': 30,
            'denominator': 577,
            'calc_value': '5.199'
        }
        self.assertEqual(q['numerator'], bsa['numerator'])
        self.assertEqual(q['denominator'], bsa['denominator'])
        self.assertEqual("%.3f" % q['calc_value'], bsa['calc_value'])

        q = self.retrieve_data_for_measure('ktt10_uti_antibiotics', 'A81001')
        bsa = {
            'numerator': 579.833,
            'denominator': 72,
            'calc_value': '8.053'
        }
        self.assertEqual("%.3f" % q['numerator'], str(bsa['numerator']))
        self.assertEqual(q['denominator'], bsa['denominator'])
        # Note that BSA divides the value by 100, for no obvious reason.
        self.assertEqual("%.3f" % (q['calc_value'] / 100), bsa['calc_value'])

        # Note practice A81005, as A81001 does not appear in published data
        q = self.retrieve_data_for_measure('ktt11_minocycline', 'A81006')
        bsa = {
            'numerator': 28,
            'denominator': 12.235 * 3,
            'calc_value': (28 / (12.235 * 3)) * 100
        }
        self.assertEqual(q['numerator'], bsa['numerator'])
        self.assertEqual(q['denominator'], bsa['denominator'])
        self.assertEqual("%.2f" % q['calc_value'], "%.2f" % bsa['calc_value'])

        q = self.retrieve_data_for_measure(
            'ktt12_diabetes_blood_glucose', 'A81001')
        bsa = {
            'numerator': 543,
            'denominator': 626,
            'calc_value': '86.741'
        }
        self.assertEqual(q['numerator'], bsa['numerator'])
        self.assertEqual(q['denominator'], bsa['denominator'])
        self.assertEqual("%.3f" % q['calc_value'], bsa['calc_value'])

        q = self.retrieve_data_for_measure('ktt12_diabetes_insulin', 'A81001')
        bsa = {
            'numerator': 44,
            'denominator': 64,
            'calc_value': '68.750'
        }
        self.assertEqual(q['numerator'], bsa['numerator'])
        self.assertEqual(q['denominator'], bsa['denominator'])
        self.assertEqual("%.3f" % q['calc_value'], bsa['calc_value'])

        q = self.retrieve_data_for_measure('ktt13_nsaids_ibuprofen', 'A81001')
        bsa = {
            'numerator': 356,
            'denominator': 413,
            'calc_value': '86.199'
        }
        self.assertEqual(q['numerator'], bsa['numerator'])
        self.assertEqual(q['denominator'], bsa['denominator'])
        self.assertEqual("%.3f" % q['calc_value'], bsa['calc_value'])

    def test_measure_by_ccg(self):
        pass

if __name__ == '__main__':
    unittest.main()
