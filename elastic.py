import json
import os
import requests as rq
import sys


class BonsaiStorage(object):
    def __init__(self):
        self.base_url = os.environ['BONSAI_URL']
        self.cluster = rq.get(self.base_url, verify=False).json()

        print 'Elasticsearch connector online'
        print '  cluster "%s": %s %s' % (self.cluster['name'], self.cluster['cluster_name'], self.cluster['version']['number'])

    def _safe_url(self, url):
        return url.split('@')[-1]

    def put(self, index, type, id, obj):
        # form a request
        url = '/'.join([self.base_url, index, type, id])
        obj = obj if obj.__class__ == dict else obj.__dict__
        try:
            doc = json.dumps(obj)
        except UnicodeDecodeError:
            print 'Failed to put object:', obj
            sys.stdout.flush()
            return None

        # commit
        r = rq.put(url, data=doc)
        return self._unpack_response(r)

    def assert_mapping(self, index, type, mapping):
        # check if the index is there
        url = '/'.join([self.base_url, index, '_mapping'])
        r = rq.get(url)
        if r.status_code == 404:
            print 'creating the index...'
            url = '/'.join([self.base_url, index])
            r = rq.put(url)
            if r.status_code >= 300:
                print 'FAILED'
                return False
            print 'created'

        # set up the mapping
        print 'putting a mapping for %s/%s...' % (index, type)
        sys.stdout.flush()
        url = '/'.join([self.base_url, index, '_mapping', type])
        r = rq.put(url, json.dumps(mapping))
        success = self._unpack_response(r) is not None
        print 'OK' if success else 'FAILED!'
        sys.stdout.flush()

        return success

    def _unpack_response(self, r):
        if r.status_code not in (200, 201):
            print 'ERROR: request to %s failed!' % self._safe_url(r.url)
            try:
                print '  problem is: ', r.json()
            except:
                print '  problem is: ', r.text
            sys.stdout.flush()
            return None
        else:
            return r.json()

    def field_stats(self, index, field):
        # fetch stats
        url = '/'.join([self.base_url, index, '_field_stats?fields=' + field])
        r = rq.get(url)
        ans = self._unpack_response(r)
        if ans is None:
            return None

        # unpack it
        stats = ans['indices']['_all']['fields']
        return stats[field] if field in stats else None
