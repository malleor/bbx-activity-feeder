import os
import requests as rq


class BonsaiStorage(object):
    def __init__(self):
        self.base_url = os.environ['BONSAI_URL']
        self.cluster = rq.get(self.base_url, verify=False).json()

        print 'Elasticsearch connector online'
        print '  cluster "%s": %s %s' % (self.cluster['name'], self.cluster['cluster_name'], self.cluster['version']['number'])
