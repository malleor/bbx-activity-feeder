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
        for jira_issue in issues:
            # convert
            issue = CreatedIssuesFeed.Issue(jira_issue)

            # store
            res = storage.put(self.index, self.type, issue.issue_key, issue)
            if res is not None:
                print 'commited the issue', issue.issue_key, 'to the datastore'
            else:
                print 'ERROR committing the issue', issue.issue_key

        self.last_called = now

# def
