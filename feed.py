from jira import JiraConnector
from apscheduler.schedulers.blocking import BlockingScheduler
from jira_feeds import *
from elastic import BonsaiStorage
import logging
import json
import os


class Scheduler(BlockingScheduler):
    def schedule(self, job, jira, storage, **trigger_args):
        name = str(job)
        return self.add_job(job, args=(jira, storage), trigger='interval', id=name, name=name, **trigger_args)


logging.basicConfig()

if __name__ == '__main__':
    # greetings
    print 'hello there'
    sys.stdout.flush()

    # prep components
    jira = JiraConnector()
    scheduler = Scheduler()
    storage = BonsaiStorage()

    # prep the feeders
    created_pace = json.loads(os.environ['CREATED_PACE'])
    feeders = [
        scheduler.schedule(CreatedIssuesFeed(), jira, storage, **created_pace)
    ]

    print 'Configured feeders:', '\n  '.join([''] + [f.name for f in feeders])
    print 'starting the scheduler'
    sys.stdout.flush()
    scheduler.start()

    print 'application is done'
    sys.stdout.flush()
