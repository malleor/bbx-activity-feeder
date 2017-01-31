import requests as rq
import sys
import os
import json


class JiraConnector(object):
    def __init__(self):
        self.base = os.environ['JIRA_BASE']
        self.user = os.environ['JIRA_USER']
        self.password = os.environ['JIRA_PASS']
        self.headers = None

        from requests.packages.urllib3.exceptions import InsecurePlatformWarning, InsecureRequestWarning, SNIMissingWarning
        from requests.packages.urllib3 import disable_warnings
        disable_warnings(InsecurePlatformWarning)
        disable_warnings(InsecureRequestWarning)
        disable_warnings(SNIMissingWarning)

        self.auth()
        print 'Jira connector online'
        print '  address:', self.base

    def auth(self):
        url = self.base + r'rest/auth/1/session'
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps({"username": self.user, "password": self.password})

        r = rq.post(url, data=payload, headers=headers, verify=False)
        assert r.status_code == 200, 'Auth request failed!'

        session = r.json()['session']
        cookie = session['name'] + '=' + session['value']

        self.headers = {'Content-Type': 'application/json', 'Cookie': cookie}

    def get(self, api, params=None, payload=None):
        self.auth()

        r = rq.get(self.base + 'rest/api/2/' + api,
                   headers=self.headers,
                   params=params,
                   data=payload,
                   verify=False)

        if r.status_code not in (200, 201):
            print 'GET request to %s failed!' % api
            try:
                print '  [%s] problem: ' % r.status_code, r.json()
            except:
                print r.text.encode('utf-8')

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
        if result is None:
            print 'failed to fetch issues'
            return []
        total = result['total']
        issues = result['issues']
        if verbose:
            print 'fetched %d of %d issues' % (len(issues), total)
            sys.stdout.flush()

        # fetch all issues batch-by-batch
        while len(issues) < total:
            params['startAt'] = len(issues)
            result = self.get('search', params=params)
            if result is None:
                print 'failed to fetch issues'
                return []
            issues += result['issues']
            if verbose:
                print 'fetched %d of %d issues' % (len(issues), total)
                sys.stdout.flush()

        return issues
