import simple_salesforce

import sys
from pathlib import Path

import requests, time, csv, io
from xml.etree import ElementTree as ET
from datetime import datetime, timezone

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    SF_USERNAME,
    SF_PASSWORD,
    SF_TOKEN,
    SF_LOGIN_URL,
)

def connect_simple_salesforce():
    simple_call = simple_salesforce.Salesforce(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_TOKEN,
    )

    return simple_call
    
class SalesforceBulkCall:
    def __init__(
        self,
        username=SF_USERNAME,
        password=SF_PASSWORD,
        token=SF_TOKEN,
        login_url=SF_LOGIN_URL,
        simple_call=connect_simple_salesforce(),
    ):

        self.simple_call = simple_call
        self.username = username
        self.password = password
        self.token = token
        self.login_url = login_url
        
        soap_body = f'''
        <env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
        <env:Body>
            <n1:login xmlns:n1="urn:partner.soap.sforce.com">
            <n1:username>{SF_USERNAME}</n1:username>
            <n1:password>{SF_PASSWORD}{SF_TOKEN}</n1:password>
            </n1:login>
        </env:Body>
        </env:Envelope>
        '''

        headers = {'Content-Type': 'text/xml; charset=UTF-8', 'SOAPAction': 'login'}
        response = requests.post(SF_LOGIN_URL, data=soap_body, headers=headers)

        tree = ET.fromstring(response.text)
        server_url = tree.find('.//{urn:partner.soap.sforce.com}serverUrl').text
        instance_url = server_url.split('/services')[0]

        self.session_id = tree.find('.//{urn:partner.soap.sforce.com}sessionId').text
        self.job_url = f'{instance_url}/services/data/v59.0/jobs/query'

    def yield_records(self, object_name, incremental_string=None):
        print('\n', '  ', object_name)
        if incremental_string:
            where_clause = f''' where SystemModstamp > {incremental_string}'''
        else:
            where_clause = ''

        obj_desc = self.simple_call.__getattr__(object_name).describe()

        compound_fields = list(set([ field['compoundFieldName'] for field in obj_desc['fields'] if field['compoundFieldName'] is not None ]))
        fields = [ field['name'] for field in obj_desc['fields'] if field['name'] not in compound_fields ]

        query = f'select {', '.join(fields)} from {object_name}' + where_clause

        job_response = requests.post(
            self.job_url,
            headers={'Authorization': f'Bearer {self.session_id}', 'Content-Type': 'application/json'},
            json={'operation': 'query', 'query': query}
        )

        job_id = job_response.json()['id']
        status_url = f'{self.job_url}/{job_id}'

        while True:
            response = requests.get(status_url, headers={'Authorization': f'Bearer {self.session_id}'})
            state = response.json()['state']
            print('      Job state:', state)
            if state in ['JobComplete', 'Failed', 'Aborted']:
                break
            time.sleep(2)

        if state != 'JobComplete':
            raise Exception('\n',f'     Job did not succeed: {state}')
        else:
            print('\n')

        result_url = f'{status_url}/results'
        results = requests.get(result_url, headers={'Authorization': f'Bearer {self.session_id}'}, stream=True)
        lines = results.content.decode('utf-8')
        reader = csv.DictReader(io.StringIO(lines))

        for record in reader:
            record['_dlt_processed_utc'] = datetime.now(timezone.utc)
            yield record

if __name__ == '__main__':
    con = SalesforceBulkCall()
    a = con.yield_records(object_name='Contact')
