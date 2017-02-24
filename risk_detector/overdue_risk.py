import numpy as np
from collections import Counter
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OneHotEncoder
IssueActivityFeed = __import__('../jira_feeds')['IssueActivityFeed']


transitions = [
  (u'Przydziel', u'FROM "Nowe" TO "Przydzielono"'),
  (u'Realizuj', u'FROM "Przydzielono" TO "W realizacji"'),
  (u'Nie moje', u'FROM "Przydzielono" TO "Do Weryfikacji"'),
  (u'Przydziel', u'FROM "Do Weryfikacji" TO "Nowe"'),
  (u'Przydziel', u'FROM "Do Weryfikacji" TO "Przydzielono"'),
  (u'Przeka\u017c', u'FROM "W realizacji" TO "Nowe"'),
  (u'Przeka\u017c', u'FROM "W realizacji" TO "Przydzielono"'),
  (u'Rozwi\u0105\u017c', u'FROM "W realizacji" TO "rozwi\u0105zane"'),
  (u'Wyja\u015bnij', u'FROM "W realizacji" TO "DO WYJA\u015aNIENIA"'),
  (u'Akceptuj', u'FROM "rozwi\u0105zane" TO "Zamkni\u0119te"'),
  (u'Odrzu\u0107', u'FROM "rozwi\u0105zane" TO "Odrzucone"'),
  (u'Realizuj', u'FROM "Odrzucone" TO "W realizacji"'),
  (u'Eskaluj', u'FROM "Odrzucone" TO "Do Weryfikacji"'),
  (u'Wyja\u015bnione', u'FROM "DO WYJA\u015aNIENIA" TO "Nowe"'),
  (u'Wyja\u015bnione', u'FROM "DO WYJA\u015aNIENIA" TO "W realizacji"'),
  (u'Archiwizuj', u'FROM "Zamkni\u0119te" TO "Zarchiwizowane"'),
  (u'Otw\xf3rz ponownie', u'FROM "Zamkni\u0119te" TO "Odrzucone"'),
]
t_ = dict([x[::-1] for x in transitions])
xcol = t_.keys()


trans = lambda h: 'FROM %s TO %s' % (h['from'].join('""'), h['to'].join('""'))
elapsed = lambda h: (h['created'] - h['last'])/1000


ik = []


# prep issue activity
feed = IssueActivityFeed()
isact = []
for i in range(100,len(ik),150):
    jql = 'key in ' + ','.join(ik[len(isact):i]).join('()')
    isact += list(feed.get_issue_activity(j, jql))
    print 'got', len(isact), 'already'

# prep transitions
# def get_transitions(issue):
#     hist = issue['changelog']['histories']
#     for h in hist:
#             for i in h['items']:
#                     if 'status' == i['field']:
#                             yield 'FROM %s TO %s' % (i['fromString'].join('""'), i['toString'].join('""'))
# issues = []
# for i in range(100,len(ik),100):
#     jql = 'key in ' + ','.join(ik[len(issues):i]).join('()')
#     issues += feed.get_issue_activity(j, jql)
#     print 'got', len(issues), 'already'
# freq = dict([(issue['key'], Counter([t for t in get_transitions(issue)])) for issue in issues])
freq = dict([(key, Counter([trans(t) for t in hist])) for key, hist, _ in isact])
fr = [freq[key] for key in ik]

# prep time measures
tm = h.get_time_measures(jql='project=bluebox and status in (10900,10608)', day=1, verbose=True)
tm2 = [x for x in tm if x and x.resolved and x.due]
tm3 = dict([(x.issue_key, x) for x in tm2])
tm4 = [tm3[key] for key in ik]

# prep fields
feature_fields = {
    'business area': 'customfield_10715',
    'channel': 'customfield_10760',
    'resolving line': 'customfield_10670',
    'issue type': 'customfield_10748',
    'issue subtype': 'customfield_10722',
}
issue_fields = sum([j.get_issues(','.join(ik[n:m]).join(['key in (',')']), verbose=True, fields=feature_fields.values()) for n, m in zip(range(0,len(ik),100),range(100,100+len(ik),100))], [])
field_value_defs = dict([(name, list(set([i['fields'][cf] for i in issue_fields]))) for name, cf in feature_fields.iteritems()])
field_values = dict([(name, np.array([field_value_defs[name].index(i['fields'][cf]) for i in issue_fields])) for name, cf in feature_fields.iteritems()])
cat_features = np.array([field_values[_feat] for _feat in feature_fields.keys()]).T
cat_features = OneHotEncoder().fit(cat_features).transform(cat_features)  # one-hot encoded
flut = dict(zip([i['key'] for i in issue_fields], cat_features))
fields = np.array([flut[key] for key in ik])


def get_all_fields(jql):
    for issue in j.get_issues(jql, changelog=True, verbose=True, fields='all'):
        yield issue['key'], dict([(n, v) for n, v in issue['fields'].iteritems() if n.find('customfield') == 0])


# prep features
def features(_freq, _act, invalid=0):
    f = [_freq.get(transname,0) for transname in xcol]
    a = dict([(transname,[]) for transname in xcol])
    for h in _act[1]:
        a[trans(h)].append(elapsed(h))
    a1 = [np.average(a.get(transname) or [invalid]) for transname in xcol]
    a2 = [np.min(a.get(transname) or [invalid]) for transname in xcol]
    a3 = [np.max(a.get(transname) or [invalid]) for transname in xcol]
    return f + a1 + a2 + a3
feature_names = ['# '+c for c in xcol] + \
                ['avg time before '+c for c in xcol] + \
                ['min time before '+c for c in xcol] + \
                ['max time before '+c for c in xcol] + \
                sum([[fname+': '+fval for fval in fvalues] for fname, fvalues in field_value_defs.iteritems()], [])
buck = {0:[],1:[],2:[]}
y = lambda x: 0 if x.resolved < x.due else (1 if x.resolved == x.due else (2 if x.resolved > x.due else 3))
# y = lambda x: 0 if x['resolved'] < x['due'] else (1 if x['resolved'] == x['due'] else (2 if x['resolved'] > x['due'] else 3))
for _freq, _due, _act in zip(fr, tm4, isact):
    buck[y(_due)].append(features(_freq, _act))

# prep the datasets
N = 79
bag = dict([(y, np.array(x, dtype=int)[np.random.permutation(len(x))][:N]) for y, x in buck.iteritems()])
X = np.vstack([np.array(bag[y]) for y in bag.keys()])
Y = np.hstack([np.array([y]*N) for y in bag.keys()])
train_size = len(Y)*8/10
perm = np.random.permutation(len(Y))
X1, X2, Y1, Y2 = X[perm][:train_size], X[perm][train_size:], Y[perm][:train_size], Y[perm][train_size:]

# train it
clf = RandomForestClassifier(n_estimators=100, criterion='entropy')
clf.fit(X1, Y1)
print 'SCORE:', clf.score(X2, Y2)


all_thresholds = {}
for t in clf.estimators_:
    thresholds = dict([(f, th) for l, f, th in zip(t.tree_.children_left, t.tree_.feature, t.tree_.threshold) if l >= 0])
    for f in thresholds.keys():
        all_thresholds[f] = all_thresholds.get(f, []) + [thresholds[f]]
all_thresholds = dict([(feature_names[fid], np.array(thr)) for fid, thr in all_thresholds.iteritems()])


def top_features(clf, top=10):
    fimp = zip(feature_names, clf.feature_importances_)
    print 'TOP %d features:' % top
    for f, i in sorted(fimp, key=lambda fi: -fi[1])[:top]:
        print '  %s%%   ' % round(100*i, 1), f.encode('utf-8')
        thr = all_thresholds[f]
        print '     thr:   %.1f .. %.1f' % (np.min(thr), np.max(thr))
        print
    return fimp


def explore_confidence_threshold(step=.1):
    for conf_th in np.arange(.2, .8, step):
        Ypp = clf.predict_proba(X2)
        Yc = np.absolute(Ypp[:, 0]-Ypp[:, 1])  # confidence measure
        good = Yc > conf_th
        X2g, X2b = X2[good], X2[np.negative(good)]
        Y2g, Y2b = Y2[good], Y2[np.negative(good)]
        print 'threshold', conf_th, \
            '  good: %.3f (%.0f%% samples)' % (clf.score(X2g, Y2g) if len(Y2g) > 0 else 0., len(Y2g)*100./len(Y2)), \
            '  bad: %.3f (%.0f%% samples)' % (clf.score(X2b, Y2b) if len(Y2b) > 0 else 0., len(Y2b)*100./len(Y2))


Ypp = clf.predict_proba(X2)
conf_th = .35
confusion = np.zeros((2,2))
for c, yp, ypp, yt in zip(np.abs(Ypp[:,0] - Ypp[:,1]), np.argmax(Ypp, axis=1), Ypp, Y2):
    if c > conf_th:
        print '~~~ %s ~~~  for %.0f%%        %s' % (['NEGATIVE', 'POSITIVE'][yp], 100.*ypp[yp], yp == yt)
        confusion[yp, yt] += 1
    else:
        print ' ?'
print 'SCORE with confidence:', np.sum(np.diag(confusion)) / np.sum(confusion)
