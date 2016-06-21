"""
Pull all available practice email addresses
"""
from lxml import html
import requests
import datetime
import json
from retrying import retry, RetryError
import time

from basecommand import BaseCommand

def retry_if_error(response):
    return response.status_code != 200

class Command(BaseCommand):

    @retry(wait_exponential_multiplier=1000,
           retry_on_result=retry_if_error,
           stop_max_attempt_number=1,
           wait_incrementing_start=0.5)
    def get_page(self, url):
        print "Trying %s" % url
        result = requests.get(url)
        return result

    def handle(self):
        min_id = 36463  # 54
        max_id = 110367

        contact_page = "http://www.nhs.uk/Services/GP/MapsAndDirections/DefaultView.aspx?id=%s"
        staff_page = "http://www.nhs.uk/Services/GP/Staff/DefaultView.aspx?id=39327"

        date_folder = datetime.datetime.today().strftime("%Y_%m")
        data = 'data/nhs_choices/%s/' % date_folder
        self.mkdir_p(data)

        with open(data + 'details.json', 'wb') as f:
            for _id in range(min_id, max_id + 100):
                time.sleep(0.5)
                datum = {'id': _id}
                page = contacts = None
                try:
                    page = self.get_page(contact_page % _id)
                except RetryError:
                    print "Couldn't get it"
                    continue
                content = html.fromstring(page.content)
                try:
                    contacts = content.xpath('//*[@id="ctl00_ctl00_ctl00_PlaceHolderMain_contentColumn1"]/div[1]/div/div/div[1]')[0]
                except IndexError:
                    hidden = not not content.xpath("//*[@id='aliasbox']/h1[text() = 'Profile Hidden']")
                    if hidden:
                        print "Profile hidden"
                        continue
                    else:
                        print "Other error: %s" % content.text_content()
                        continue
                datum['name'] = content.xpath('//*[@id="org-title"]/text()')[0]
                headings = ['Tel', 'Fax', 'Address', 'Email', 'Website']
                for heading in headings:
                    try:
                        if heading == "Email":
                            val = contacts.xpath("//strong[contains(text(),'%s')]/following-sibling::a/text()" % heading)[0].strip()
                        else:
                            val = contacts.xpath("//strong[contains(text(),'%s')]/following-sibling::text()[1]" % heading)[0].strip()
                    except IndexError:
                        val = ""
                    datum[heading.lower()] = val
                print json.dumps(datum)
                f.write(json.dumps(datum) + "\n")
if __name__ == '__main__':
    Command().handle()
