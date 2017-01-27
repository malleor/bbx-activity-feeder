from jira import JiraConnector
import os
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
from jira_feeds import *
from elastic import BonsaiStorage


class Scheduler(BlockingScheduler):
    def schedule(self, job, jira, **trigger_args):
        name = job.func_name
        return self.add_job(job, args=(jira,), trigger='interval', id=name, name=name, **trigger_args)


if __name__ == '__main__':
    # greetings
    print 'hello there'
    sys.stdout.flush()

    # prep components
    jira = JiraConnector()
    scheduler = Scheduler()
    storage = BonsaiStorage()

    feeders = [
        scheduler.schedule(get_todays_issues, jira, seconds=20)
    ]

    print 'configured feeders:  ', ', '.join([f.name for f in feeders])
    print 'starting the scheduler'
    sys.stdout.flush()
    scheduler.start()

    print 'application is done'
    sys.stdout.flush()
