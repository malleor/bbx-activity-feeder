import os
import sys


PROJECT_NAME = os.environ['JIRA_PROJECT']


def get_todays_issues(jira):
    issues = jira.get_issues('project=%s and created>=startOfDay()' % PROJECT_NAME, verbose=True)

    print 'DONE:', len(issues), 'issues'
    sys.stdout.flush()


# def
