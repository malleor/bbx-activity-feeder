import os
import sys
from datetime import datetime
import time
from dateutil.parser import parse as parse_date


PROJECT_NAME = os.environ['JIRA_PROJECT']


class CreatedIssuesFeed(object):
    def __init__(self):
        self.index = PROJECT_NAME.lower()
        self.type = 'created8'

        self.ELASTIC_MAPPING = {
            self.type: {
                'properties': {
                    'created': {
                        'type': 'date'
                    },
                    'issue_key': {
                        'type': 'string'
                    },
                    'channel': {
                        'type': 'string'
                    },
                    'biz': {
                        'type': 'string'
                    }
                }
            }
        }
        self.JIRA_MAPPING = {
            'channel': 'customfield_10760',
            'biz': 'customfield_10715'
        }

    class Issue(object):
        def __init__(self, jira_issue, mapping):
            jira_fields = jira_issue['fields']
            created_str = jira_fields['created'][:-5]
            created_date = datetime.strptime(created_str, '%Y-%m-%dT%H:%M:%S.%f')
            created_epoch = 1000*int(time.mktime(created_date.timetuple()))

            self.created = created_epoch
            self.issue_key = jira_issue['key']
            self.biz = jira_fields[mapping['biz']]
            self.channel = jira_fields[mapping['channel']]

    def __call__(self, jira, storage):
        # assert that the storage is ready for receiving docs
        if not storage.assert_mapping(self.index, self.type, self.ELASTIC_MAPPING):
            print 'mapping not set; dropping the feed'
            sys.stdout.flush()
            return

        # check since when should the issues be fetched
        stats = storage.field_stats(self.index, 'created')
        if stats:
            last_created = datetime.fromtimestamp(stats['max_value'] / 1000).strftime('"%Y-%m-%d %H:%M"')
            print 'the freshest issue was created', last_created
        else:
            last_created = "startOfDay()"
            print 'could not fetch stats from the storage; using', last_created

        # fetch issues
        issues = jira.get_issues('project=%s and created>=%s order by created asc' % (PROJECT_NAME, last_created),
                                 fields=['created'] + self.JIRA_MAPPING.values(),
                                 verbose=True)
        print 'got', len(issues), 'issues'
        sys.stdout.flush()

        # store the issues
        erroneous_issues = []
        n = len(issues) - 1
        if n >= 0:
            if n == 0:
                n = 1
            BAR_LENGTH = 30
            for i, jira_issue in enumerate(issues):
                # convert
                issue = CreatedIssuesFeed.Issue(jira_issue, self.JIRA_MAPPING)

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


class IssueActivityFeed(object):
    def __init__(self):
        self.index = PROJECT_NAME.lower()
        self.type = 'activity2'

        self.ELASTIC_MAPPING = {
            self.type: {
                'properties': {
                    'created': {
                        'type': 'date'
                    },
                    'last': {
                        'type': 'date'
                    }
                }
            }
        }
        self.JIRA_MAPPING = {
        }

    def __call__(self, jira, storage, day=''):
        # assert that the storage is ready for receiving docs
        if not storage.assert_mapping(self.index, self.type, self.ELASTIC_MAPPING):
            print 'mapping not set; dropping the feed'
            sys.stdout.flush()
            return

        jql = 'project=%s and created>=startofday(%s) and created<endofday(%s)' % (PROJECT_NAME, day, day)
        issues = jira.get_issues(jql=jql,
                                 fields=['created', 'assignee'],
                                 changelog=True,
                                 verbose=True)

        total_activities = 0
        for issue in issues:
            key = issue['key']
            try:
                last_assignee = issue['fields']['assignee']['name']
            except TypeError:
                print 'no assignee in issue', key, '???'
                sys.stdout.flush()
                continue
            created = parse_date(issue['fields']['created'])

            def _field(f, fields, timestamp):
                if f in fields.keys():
                    return timestamp, (fields[f]['fromString'] or ''), (fields[f]['toString'] or '')
                return None

            def _extract(h):
                fields = dict([(i['field'], i) for i in h['items']])
                timestamp = parse_date(h['created'])
                return _field('assignee', fields, timestamp), _field('status', fields, timestamp)

            # extract status and assignee changes from the changelog
            history = [_extract(h) for h in issue['changelog']['histories']]
            history = [(a, s) for a, s in history if a or s]
            # for a, s in history:
            #     print 'assignee:' if a else 'status:', a or s

            # set up the assignee
            assignee = last_assignee
            history = list(reversed(history))
            for ix, h in enumerate(history):
                a, s = h
                if a:
                    assignee = a[2]
                    history[ix] = None
                else:
                    t, s1, s2 = s
                    history[ix] = {
                        'key': key,
                        'created': t,
                        'assignee': assignee,
                        'from': s1,
                        'to': s2
                    }
            history = [h for h in reversed(history) if h]

            # set previous change time
            previous = created
            for h in history:
                h['last'], previous = previous, h['created']
                h['created'] = 1000*int(time.mktime(h['created'].timetuple()))
                h['last'] = 1000*int(time.mktime(h['last'].timetuple()))

            # for h in history:
            #     print h['timestamp']-h['previous'], h['assignee'], h['from'], 'to', h['to']

            # store
            for ix, h in enumerate(history):
                res = storage.put(self.index, self.type, '%s@%d' % (h['key'], ix), h)
                if res is None:
                    print 'ERROR dealing with change', ix
            total_activities += len(history)
            print 'shoved', len(history), 'entries for issue', key
            sys.stdout.flush()
        print 'shoved', total_activities, 'entries for', len(issues), 'issues'
        sys.stdout.flush()
