import os
import sys
from datetime import datetime


PROJECT_NAME = os.environ['JIRA_PROJECT']


class CreatedIssuesFeed(object):
    def __init__(self):
        self.last_called = None
        self.index = PROJECT_NAME.lower()
        self.type = 'created'

        self.mapping = {
            self.type: {
                'properties': {
                    'created': {
                        'type': 'date'
                    },
                    'issue_key': {
                        'type': 'string'
                    }
                }
            }
        }

    class Issue(object):
        def __init__(self, jira_issue):
            self.created = jira_issue['fields']['created']
            self.issue_key = jira_issue['key']

    def __call__(self, jira, storage):
        # check the clock
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        if self.last_called is None:
            print 'dry run at', now
            sys.stdout.flush()
            self.last_called = now
            return

        # assert that the storage is ready for receiving docs
        if not storage.assert_mapping(self.index, self.mapping):
            print 'mapping not set; dropping the feed'
            sys.stdout.flush()
            return

        # fetch issues
        issues = jira.get_issues('project=%s and created>="%s"' % (PROJECT_NAME, self.last_called),
                                 fields=('created',),
                                 verbose=True)
        print 'got', len(issues), 'issues'
        sys.stdout.flush()

        # store the issues
        erroneous_issues = []
        n = len(issues) - 1
        BAR_LENGTH = 30
        for i, jira_issue in enumerate(issues):
            # convert
            issue = CreatedIssuesFeed.Issue(jira_issue)

            # store
            res = storage.put(self.index, self.type, issue.issue_key, issue)
            if res is None:
                erroneous_issues.append(issue.issue_key)

            # log progress
            if (i * 100 / n) % 10 == 0:
                u = i * BAR_LENGTH / n
                v = BAR_LENGTH + 1 - u
                print '|' + u * '=' + '>' + v * ' ' + '|'
                sys.stdout.flush()
        print 'stored', len(issues), 'issues'
        if len(erroneous_issues) > 0:
            print len(erroneous_issues), 'FAILED:', '\n  '.join([''] + erroneous_issues)
        sys.stdout.flush()

        self.last_called = now

# def
