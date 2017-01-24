from jira import JiraConnector
import os
import sys
from apscheduler.schedulers.blocking import BlockingScheduler


class Feeder(object):
    def __init__(self, data_source, scheduler, job, sched_args):
        self.name = job_name = job.func_name

        @scheduler.scheduled_job('interval', **sched_args)
        def job_wrapper():
            print 'running a scheduled job', job_name
            sys.stdout.flush()
            job(data_source)
            sys.stdout.flush()


def get_todays_issues(jira):
    issues = jira.get_issues('project=BlueBox and created>=startOfDay()', verbose=True)
    print 'DONE:', len(issues), 'issues'


print 'hello there'
sys.stdout.flush()

assert 'JIRA_PASS' in os.environ, 'I need JIRA_PASS in the environment to access Jira'
JIRA_PASS = os.environ['JIRA_PASS']


jira = JiraConnector(JIRA_PASS)
scheduler = BlockingScheduler()

feeders = [
    Feeder(jira, scheduler, get_todays_issues, {'seconds': 20})
]

print 'configured feeders:  ', ', '.join([f.name for f in feeders])
print 'starting the scheduler'
sys.stdout.flush()
scheduler.start()

print 'application is done'
sys.stdout.flush()
