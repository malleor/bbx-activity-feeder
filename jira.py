import requests as rq
import sys
import os
import json


class JiraConnector(object):
    def __init__(self):
        self.base = os.environ['JIRA_BASE']
        self.user = os.environ['JIRA_USER']
        password = os.environ['JIRA_PASS']

        from requests.packages.urllib3.exceptions import InsecurePlatformWarning, InsecureRequestWarning, SNIMissingWarning
        from requests.packages.urllib3 import disable_warnings
        disable_warnings(InsecurePlatformWarning)
        disable_warnings(InsecureRequestWarning)
        disable_warnings(SNIMissingWarning)

        cookie = self.prep_cookie(password)
        self.headers = {'Content-Type': 'application/json', 'Cookie': cookie}

        print 'Jira connector online'
        print '  address:', self.base

    def prep_cookie(self, password):
        url = self.base + r'rest/auth/1/session'
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps({"username": self.user, "password": password})

        r = rq.post(url, data=payload, headers=headers, verify=False)
        assert r.status_code == 200, 'Auth request failed!'

        session = r.json()['session']
        cookie = session['name'] + '=' + session['value']

        return cookie

    def get(self, api, params=None, payload=None):
        r = rq.get(self.base + 'rest/api/2/' + api,
                   headers=self.headers,
                   params=params,
                   data=payload,
                   verify=False)
        if r.status_code not in (200, 201):
            print 'GET request to %s failed!' % api
            try:
                print '  problem: ', r.json()
            except:
                print '  problem: ', r.text
            return None
        else:
            return r.json()

    def get_issues(self, jql, fields=None, changelog=False, verbose=False):

        # form a request
        params = {
            'jql': jql,
            'fields': ','.join(fields or ['key']),
            'fieldsByKeys': True,
            'startAt': 0,
            'expand': 'changelog' if changelog  else ''
        }
        if verbose:
            print 'prepared the request:'
            print '  ', params['jql']
            sys.stdout.flush()

        # fetch a batch of issues
        result = self.get('search', params=params)
        total = result['total']
        issues = result['issues']
        if verbose:
            print 'fetched %d of %d issues' % (len(issues), total)
            sys.stdout.flush()

        # fetch all issues batch-by-batch
        while len(issues) < total:
            params['startAt'] = len(issues)
            result = self.get('search', params=params)
            issues += result['issues']
            if verbose:
                print 'fetched %d of %d issues' % (len(issues), total)
                sys.stdout.flush()

        return issues
