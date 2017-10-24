from collections import Counter
from datetime import datetime, timedelta
import numpy as np


class Customfields:
    line =        'customfield_10670'
    channel =     'customfield_10760'
    kpi =         'customfield_10653'
    duedate =     'customfield_10624'
    regdate1 =    'customfield_10617'
    regdate2 =    'customfield_10613'
    resdate =     'customfield_10612'
    biz =         'customfield_10715'
    status =      'status'
    assignee =    'assignee'


class TimeMeasure(object):
    def __init__(self, now, issue_key, start, due, resolved, kpi, status, biz, assignee):
        self.kpi = int(kpi or 0)
        self.assignee = assignee
        self.biz = biz
        self.status = status
        self.due = due
        self.resolved = resolved
        self.start = start
        self.issue_key = issue_key

        # how many days are we behind? (now - duedate)
        exact_due = datetime.strptime(due, '%Y-%m-%d') + timedelta(days=1)
        self.overdue_days = (now - exact_due).days + 1
        self.now = now.strftime('%Y-%m-%d')


class VotePredictor(object):
    FETCH_BATCH_SIZE = 150

    def __init__(self, jira, activity_feed):
        self.activity_feed = activity_feed
        self.jira = jira

    def fetch_votes(self, path):
        lines = open(path, 'rt').readlines()
        votes = [l[:-2].split(';')[::-1] for l in lines[1:]]
        votes = dict([(k, v == 'true') for k, v in votes])
        print 'read', len(votes), 'votes from', path

        bad = [k for k in votes.keys() if k.find('OZ')==-1]
        for k in bad:
            del votes[k]
        print 'pruned;', len(votes), 'votes left'

        return votes

    def fetch_data(self, votes):
        # fetch issues
        # need changelog and fields
        ik = votes.keys()
        jql = lambda n, m: ','.join(ik[n:m]).join(['key in (', ')'])
        batches = zip(range(0, len(ik), VotePredictor.FETCH_BATCH_SIZE),
                      range(VotePredictor.FETCH_BATCH_SIZE, VotePredictor.FETCH_BATCH_SIZE+len(ik), VotePredictor.FETCH_BATCH_SIZE))
        issues = []
        for n, m in batches:
            issues += self.jira.get_issues(jql(n, m), verbose=False, fields='all', changelog=True)
            print 'got', len(issues), 'of', len(ik)

        # extract fields
        fields = dict(self.extract_fields(issues))

        # extract timing data
        timing = dict([(k, h) for k, h, _ in self.extract_transitions(issues)])

        # form output
        for key in ik:
            if key in fields and key in timing:
                yield key, fields[key], timing[key]
            else:
                print 'hard time fetching issue', key

    def extract_fields(self, issues):
        for issue in issues:
            yield issue['key'], dict([(n, v) for n, v in issue['fields'].iteritems() if n.find('customfield') == 0])

    def extract_transitions(self, issues):
        return self.activity_feed.extract_issue_activity(issues)

    def prepare_features(self, votes, issues):
        # prep for timing processing
        trans_name = lambda f, t: u'%s - %s' % (f.join('""'), t.join('""'))
        trans = lambda h: trans_name(h['from'], h['to'])
        transitions = [
            (u'Przydziel', trans_name(u'Nowe', u'Przydzielono')),
            (u'Realizuj', trans_name(u'Przydzielono', u'W realizacji')),
            (u'Nie moje', trans_name(u'Przydzielono', u'Do Weryfikacji')),
            (u'Przydziel', trans_name(u'Do Weryfikacji', u'Nowe')),
            (u'Przydziel', trans_name(u'Do Weryfikacji', u'Przydzielono')),
            (u'Przeka\u017c', trans_name(u'W realizacji', u'Nowe')),
            (u'Przeka\u017c', trans_name(u'W realizacji', u'Przydzielono')),
            (u'Rozwi\u0105\u017c', trans_name(u'W realizacji', u'rozwi\u0105zane')),
            (u'Wyja\u015bnij', trans_name(u'W realizacji', u'DO WYJA\u015aNIENIA')),
            (u'Akceptuj', trans_name(u'rozwi\u0105zane', u'Zamkni\u0119te')),
            (u'Odrzu\u0107', trans_name(u'rozwi\u0105zane', u'Odrzucone')),
            (u'Realizuj', trans_name(u'Odrzucone', u'W realizacji')),
            (u'Eskaluj', trans_name(u'Odrzucone', u'Do Weryfikacji')),
            (u'Wyja\u015bnione', trans_name(u'DO WYJA\u015aNIENIA', u'Nowe')),
            (u'Wyja\u015bnione', trans_name(u'DO WYJA\u015aNIENIA', u'W realizacji')),
            (u'Archiwizuj', trans_name(u'Zamkni\u0119te', u'Zarchiwizowane')),
            (u'Otw\xf3rz ponownie', trans_name(u'Zamkni\u0119te', u'Odrzucone')),
        ]
        t_ = dict([x[::-1] for x in transitions])
        xcol = t_.keys()
        elapsed = lambda h: (h['created'] - h['last'])/1000

        def workflow_features(_freq, _act, invalid=0):
            f = [_freq.get(transname,0) for transname in xcol]
            a = dict([(transname, []) for transname in xcol])
            for h in _act:
                a[trans(h)].append(elapsed(h))
            a1 = [np.average(a.get(transname) or [invalid]) for transname in xcol]
            a2 = [np.min(a.get(transname) or [invalid]) for transname in xcol]
            a3 = [np.max(a.get(transname) or [invalid]) for transname in xcol]
            return f + a1 + a2 + a3

        def field_indices(fields):
            return np.array([field_value_defs[cf].index(v) if v is not None else -1 for cf, v in fields.iteritems()])

        def field_features(fields):
            return []

        # prep for field processing
        all_cfs = issues[0][1].keys()
        all_cf_values = lambda cf: [customfields[cf] for _, customfields, _ in issues if customfields[cf] is not None]
        field_value_defs = dict([(cf, list(set(all_cf_values(cf)))) for cf in all_cfs])
        all_field_indices = np.array([field_indices(fields) for _, fields, _ in issues])
        nonempty_field_indices = all_field_indices[:, np.any(all_field_indices != -1, axis=0)]
        print 'there\'s', all_field_indices.shape[1], 'fields while', nonempty_field_indices.shape[1], 'are nonempty'

        # todo ONE HOT ENCODER

        # form features per class
        classes = {True: [], False: []}
        for key, fields, timing in issues:
            frequencies = Counter([trans(t) for t in timing])
            features = workflow_features(frequencies, timing) + field_features(fields)
            classes[votes[key]].append(features)
        classes = dict([(key, np.array(features, dtype=int)) for key, features in classes.iteritems()])

        return classes

