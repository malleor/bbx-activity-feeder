# coding=utf-8
import os
import sys
import time
import random
import datetime


ELASTIC_SETTINGS = {
    "index": {
        "analysis": {
            "analyzer": {
                "default": {
                    "tokenizer": "keyword"
                }
            }
        }
    }
}


class Cluster(object):
    OCTOPUS_MAPPING = {
        "id": "id",
        "name": "name"
    }


class Service(object):
    def __init__(self, s, cluster_names):
        self.id = s[Service.OCTOPUS_MAPPING['id']]
        self.name = s[Service.OCTOPUS_MAPPING['name']]
        self.description = s[Service.OCTOPUS_MAPPING['description']]
        self.type = s[Service.OCTOPUS_MAPPING['type']]
        self.created = 1000 * int(time.mktime(s[Service.OCTOPUS_MAPPING['created']].timetuple()))
        self.updated = 1000 * int(time.mktime(s[Service.OCTOPUS_MAPPING['updated']].timetuple()))
        self.author = s[Service.OCTOPUS_MAPPING['author']]
        self.cluster = cluster_names[s[Service.OCTOPUS_MAPPING['cluster']]]
        self.repo = s[Service.OCTOPUS_MAPPING['repo']]
        self.manual = s[Service.OCTOPUS_MAPPING['manual']]

    ELASTIC_MAPPING = {
        'properties': {
            'id': {
                'type': 'long'
            },
            'type': {
                'type': 'long'
            },
            'created': {
                'type': 'date'
            },
            'updated': {
                'type': 'date'
            },
            'author': {
                'type': 'long'
            },
            'manual': {
                'type': 'long'
            },
        }
    }

    OCTOPUS_MAPPING = {
        'id': 'id',
        'name': 'title',
        'description': 'description',
        'type': 'id_type',
        'created': 'date_created',
        'updated': 'date_update',
        'author': 'author',
        'cluster': 'id_cluster',
        'repo': 'url_repository',
        'manual': 'avg_manual_time'
    }


class Execution(object):
    def __init__(self, e, service_names, service_manual_times):
        self.id = e[Execution.OCTOPUS_MAPPING['id']]
        self.service = service_names[e[Execution.OCTOPUS_MAPPING['service']]]
        self.manual = service_manual_times[e[Execution.OCTOPUS_MAPPING['service']]]

        self.created = 1000 * int(time.mktime(e[Execution.OCTOPUS_MAPPING['created']].timetuple()))
        self.started = 1000 * int(time.mktime(e[Execution.OCTOPUS_MAPPING['started']].timetuple()))
        self.finished = 1000 * int(time.mktime(e[Execution.OCTOPUS_MAPPING['finished']].timetuple()))
        # self.result = s[ExecutionsFeed.OCTOPUS_MAPPING['result']]
        self.status = Execution.OCTOPUS_STATUSES[e[Execution.OCTOPUS_MAPPING['status']]]['text']
        self.statustype = Execution.OCTOPUS_STATUSES[e[Execution.OCTOPUS_MAPPING['status']]]['class']
        self.token = e[Execution.OCTOPUS_MAPPING['token']]

    ELASTIC_MAPPING = {
        'properties': {
            'id': {
                'type': 'long'
            },
            'service': {
                'type': 'string'
            },
            'created': {
                'type': 'date'
            },
            'started': {
                'type': 'date'
            },
            'finished': {
                'type': 'date'
            },
            'manual': {
                'type': 'long'
            },
        }
    }

    OCTOPUS_MAPPING = {
        'id': 'id',
        'service': 'id_service',
        'created': 'date_add',
        'started': 'date_beg_processing',
        'finished': 'date_end_processing',
        'result': 'result',
        'status': 'status',
        'token': 'token'
    }

    OCTOPUS_STATUSES = {
        0: {'class': 'default', 'text': u'Oczekuje'},
        1: {'class': 'success', 'text': u'Zakończony'},
        2: {'class': 'danger', 'text': u'Błąd'},
        3: {'class': 'warning', 'text': u'Do sprawdzenia'},
        4: {'class': 'warning', 'text': u'Przerwany'},
        5: {'class': 'warning', 'text': u'W trakcie realizacji'}
    }


class ServicesFeed(object):
    def __init__(self):
        self.cluster_names = None

    INDEX = 'octopus'
    TYPE = 'services'

    def __call__(self, octopus, storage):
        print 'ServicesFeed start'
        sys.stdout.flush()

        # assert that the storage is ready for receiving docs
        if not storage.assert_mapping(ServicesFeed.INDEX, ServicesFeed.TYPE, Service.ELASTIC_MAPPING):
            print 'mapping not set; dropping the feed'
            sys.stdout.flush()
            return
        if not storage.assert_settings(ServicesFeed.INDEX, ELASTIC_SETTINGS):
            print 'settings not set; dropping the feed'
            sys.stdout.flush()
            return

        # fetch cluster names
        if self.cluster_names is None:
            clusters = octopus.get_clusters()
            unpack = lambda c: (c[Cluster.OCTOPUS_MAPPING['id']], c[Cluster.OCTOPUS_MAPPING['name']])
            self.cluster_names = dict([unpack(c) for c in clusters])

        # fetch services
        services = octopus.get_services()
        print '  fetched', len(services), 'services'
        sys.stdout.flush()

        # convert services
        services = [Service(s, self.cluster_names) for s in services]

        # push to the storage
        res = any([storage.put(ServicesFeed.INDEX, ServicesFeed.TYPE, str(s.id), s) is None for s in services])
        print '  shoved into the storage' if res is not None else '  FAILED to shove services into the storage'
        sys.stdout.flush()

        print 'ServicesFeed end'
        sys.stdout.flush()


class ExecutionsFeed(object):
    def __init__(self):
        self.service_names = None
        self.service_manual_times = None

    INDEX = 'octopus'
    TYPE = 'exec'

    def __call__(self, octopus, storage):
        print 'ExecutionsFeed start'
        sys.stdout.flush()

        # assert that the storage is ready for receiving docs
        if not storage.assert_mapping(ExecutionsFeed.INDEX, ExecutionsFeed.TYPE, Execution.ELASTIC_MAPPING):
            print 'mapping not set; dropping the feed'
            sys.stdout.flush()
            return
        if not storage.assert_settings(ServicesFeed.INDEX, ELASTIC_SETTINGS):
            print 'settings not set; dropping the feed'
            sys.stdout.flush()
            return

        # fetch services
        if self.service_names is None or self.service_manual_times is None:
            services = octopus.get_services()

            def fetch(service, field):
                return service[Service.OCTOPUS_MAPPING['id']], service[Service.OCTOPUS_MAPPING[field]]

            self.service_names = dict([fetch(s, 'name') for s in services])
            self.service_manual_times = dict([fetch(s, 'manual') for s in services])

        # fetch executions
        executions = octopus.get_executions()
        print '  fetched', len(executions), 'service executions'
        sys.stdout.flush()

        # convert executions
        executions = [Execution(e, self.service_names, self.service_manual_times) for e in executions]

        # push to the storage
        n = len(executions)
        failed = 0
        for i, e in enumerate(executions):
            if storage.put(ExecutionsFeed.INDEX, ExecutionsFeed.TYPE, str(e.id), e) is None:
                failed += 1
            if i % 10 == 9:
                print i - failed, 'put,', failed, 'failed'
                sys.stdout.flush()
        print '  shoved', n - failed, 'objects out of', n, 'into the storage'
        sys.stdout.flush()

        print 'ExecutionsFeed end'
        sys.stdout.flush()


class FakeDataSource(object):
    def __init__(self, octopus):
        # fetch real clusters
        self.clusters = octopus.get_clusters()
        cluster_ids = [c[Cluster.OCTOPUS_MAPPING['id']] for c in self.clusters]

        # generate fake services
        DEMO_NUM_SERVICES = int(os.environ['DEMO_NUM_SERVICES'])
        SERVICE_TIMEBOX = (datetime.datetime.now() - datetime.timedelta(weeks=8), datetime.datetime.now())
        self.services = [self._gen_service(1+i, SERVICE_TIMEBOX[0], SERVICE_TIMEBOX[1], cluster_ids) for i in range(DEMO_NUM_SERVICES)]

        # generate fake executions
        DEMO_NUM_EXECUTIONS = int(os.environ['DEMO_NUM_EXECUTIONS'])
        self.executions = [self._gen_execution(1+i, self.services) for i in range(DEMO_NUM_EXECUTIONS)]

    def _gen_service(self, id, created_beg,  created_end, cluster_ids):
        # pick a date
        timebox = (created_end - created_beg).total_seconds()
        created = created_beg + datetime.timedelta(seconds=random.uniform(0, timebox))

        # pick a cluster
        cluster = cluster_ids[random.randint(0, len(cluster_ids)-1)]

        # come up with an execution time
        manual_time = random.randint(1, 5*60)  # seconds

        # form a result
        return {
            'id': -id,
            'title': 'Demo service #%s' % id,
            'description': 'Fake service for demo purposes',
            'id_type': 42,
            'date_created': created,
            'date_update': created,
            'author': 42,
            'id_cluster': cluster,
            'url_repository': '',
            'avg_manual_time': manual_time
        }

    def _gen_execution(self, id, services):
        # pick a service
        service = services[random.randint(0, len(services)-1)]

        # come up with timing
        manual = service['avg_manual_time']
        exec_time = random.uniform(manual * .3, manual * .7)
        wait_time = random.uniform(exec_time * .2, exec_time * 2.)
        timebox = (datetime.datetime.now() - service['date_created']).total_seconds()
        created = service['date_created'] + datetime.timedelta(seconds=random.uniform(0, timebox))
        started = created + datetime.timedelta(seconds=wait_time)
        finished = started + datetime.timedelta(seconds=exec_time)

        # form a result
        return {
            'id': -id,
            'id_service': service['id'],
            'date_add': created,
            'date_beg_processing': started,
            'date_end_processing': finished,
            'result': '',
            'status': 1,
            'token': ''
        }

    def get_services(self):
        return self.services

    def get_executions(self):
        return self.executions

    def get_clusters(self):
        return self.clusters
