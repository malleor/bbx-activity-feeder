from jira import JiraConnector
from octopus import OctopusConnector
from apscheduler.schedulers.blocking import BlockingScheduler
import jira_feeds
import octopus_feeds
from elastic import BonsaiStorage
import logging
import json
import os
import sys


class Scheduler(BlockingScheduler):
    def schedule(self, job, src, dst, **trigger_args):
        name = str(job)
        return self.add_job(job, args=(src, dst), trigger='interval', id=name, name=name, **trigger_args)


logging.basicConfig()

if __name__ == '__main__':
    # greetings
    print 'Feeder starting up...'
    sys.stdout.flush()

    # prep components
    jira = JiraConnector()
    octopus = OctopusConnector()
    storage = BonsaiStorage()
    scheduler = Scheduler()

    # prep the feeders
    created_pace = json.loads(os.environ['CREATED_PACE'])
    octopus_services_pace = json.loads(os.environ['OCTOPUS_SERVICES_PACE'])
    octopus_executions_pace = json.loads(os.environ['OCTOPUS_EXECUTIONS_PACE'])
    feeders = [
        scheduler.schedule(jira_feeds.CreatedIssuesFeed(), jira, storage, **created_pace),
        scheduler.schedule(octopus_feeds.ServicesFeed(), octopus, storage, **octopus_services_pace),
        scheduler.schedule(octopus_feeds.ExecutionsFeed(), octopus, storage, **octopus_executions_pace)
    ]

    # run data initializers
    if 'OCTOPUS_PUT_FAKE_DATA' in os.environ and os.environ['OCTOPUS_PUT_FAKE_DATA'].lower() == 'true':
        fake_datasource = octopus_feeds.FakeDataSource(octopus)
        octopus_feeds.ServicesFeed()(fake_datasource, storage)
        octopus_feeds.ExecutionsFeed()(fake_datasource, storage)
    if 'BLUEBOX_FETCH_INIT_DATA' in os.environ:
        data_to_fetch = json.loads(os.environ['BLUEBOX_FETCH_INIT_DATA'])
        for day in data_to_fetch['days']:
            jira_feeds.IssueActivityFeed()(jira, storage, day)

    print 'Configured feeders:', '\n  '.join([''] + [f.name for f in feeders])
    print 'starting the scheduler'
    sys.stdout.flush()
    scheduler.start()

    print 'application is done'
    sys.stdout.flush()
