from jira import JiraConnector
import os


def feed(jira_pass):
    jira = JiraConnector(jira_pass)
    issues = jira.get_issues('project=BlueBox and created>=startOfDay()', verbose=True)
    print 'fetched', len(issues), 'issues'


assert 'JIRA_PASS' in os.environ, 'I need JIRA_PASS in the environment to access Jira'
JIRA_PASS = os.environ['JIRA_PASS']

feed(JIRA_PASS)
