from jira import JiraConnector
import os

assert 'JIRA_PASS' in os.environ, 'I need JIRA_PASS in the environment to access Jira'
assert JiraConnector(os.environ['JIRA_PASS']), 'failed to connect to Jira'
print 'healthcheck OK'
