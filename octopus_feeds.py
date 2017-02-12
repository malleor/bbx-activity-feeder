# coding=utf-8
import sys
import time


class ServicesFeed(object):
    def __init__(self):
        pass

    INDEX = 'octopus'
    TYPE = 'services'

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
            'cluster': {
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
        'repo': 'url_repository'
    }

    class Service(object):
        def __init__(self, s):
            self.id             = s[ServicesFeed.OCTOPUS_MAPPING['id']]
            self.name           = s[ServicesFeed.OCTOPUS_MAPPING['name']]
            self.description    = s[ServicesFeed.OCTOPUS_MAPPING['description']]
            self.type           = s[ServicesFeed.OCTOPUS_MAPPING['type']]
            self.created        = 1000*int(time.mktime(s[ServicesFeed.OCTOPUS_MAPPING['created']].timetuple()))
            self.updated        = 1000*int(time.mktime(s[ServicesFeed.OCTOPUS_MAPPING['updated']].timetuple()))
            self.author         = s[ServicesFeed.OCTOPUS_MAPPING['author']]
            self.cluster        = s[ServicesFeed.OCTOPUS_MAPPING['cluster']]
            self.repo           = s[ServicesFeed.OCTOPUS_MAPPING['repo']]

    def __call__(self, octopus, storage):
        print 'ServicesFeed start'
        sys.stdout.flush()

        # assert that the storage is ready for receiving docs
        if not storage.assert_mapping(ServicesFeed.INDEX, ServicesFeed.TYPE, ServicesFeed.ELASTIC_MAPPING):
            print 'mapping not set; dropping the feed'
            sys.stdout.flush()
            return

        # fetch services
        services = octopus.get_services()
        print '  fetched', len(services), 'services'
        sys.stdout.flush()

        # convert services
        services = [ServicesFeed.Service(s) for s in services]

        # push to the storage
        res = any([storage.put(ServicesFeed.INDEX, ServicesFeed.TYPE, str(s.id), s) is None for s in services])
        print '  shoved into the storage' if res is not None else '  FAILED to shove services into the storage'
        sys.stdout.flush()

        print 'ServicesFeed end'
        sys.stdout.flush()


class ExecutionsFeed(object):
    def __init__(self):
        self.service_names = None

    INDEX = 'octopus'
    TYPE = 'exec'

    ELASTIC_MAPPING = {
        TYPE: {
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
            }
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
           0: {'class': 'default', 'text': 'Oczekuje'},
           1: {'class': 'success', 'text': 'Zakończony'},
           2: {'class': 'danger',  'text': 'Błąd'},
           3: {'class': 'warning', 'text': 'Do sprawdzenia'},
           4: {'class': 'warning', 'text': 'Przerwany'},
           5: {'class': 'warning', 'text': 'W trakcie realizacji'}
    }

    class Execution(object):
        def __init__(self, e, service_names):
            self.id             = e[ExecutionsFeed.OCTOPUS_MAPPING['id']]
            self.service        = service_names[e[ExecutionsFeed.OCTOPUS_MAPPING['service']]]
            self.created        = 1000*int(time.mktime(e[ExecutionsFeed.OCTOPUS_MAPPING['created']].timetuple()))
            self.started        = 1000*int(time.mktime(e[ExecutionsFeed.OCTOPUS_MAPPING['started']].timetuple()))
            self.finished       = 1000*int(time.mktime(e[ExecutionsFeed.OCTOPUS_MAPPING['finished']].timetuple()))
            #self.result         = s[ExecutionsFeed.OCTOPUS_MAPPING['result']]
            self.status         = ExecutionsFeed.OCTOPUS_STATUSES[e[ExecutionsFeed.OCTOPUS_MAPPING['status']]]['text']
            self.statustype     = ExecutionsFeed.OCTOPUS_STATUSES[e[ExecutionsFeed.OCTOPUS_MAPPING['status']]]['class']
            self.token          = e[ExecutionsFeed.OCTOPUS_MAPPING['token']]

    def __call__(self, octopus, storage):
        print 'ExecutionsFeed start'
        sys.stdout.flush()

        # assert that the storage is ready for receiving docs
        if not storage.assert_mapping(ExecutionsFeed.INDEX, ExecutionsFeed.TYPE, ExecutionsFeed.ELASTIC_MAPPING):
            print 'mapping not set; dropping the feed'
            sys.stdout.flush()
            return

        # fetch services
        if self.service_names is None:
            services = octopus.get_services()
            self.service_names = dict([(s[ServicesFeed.OCTOPUS_MAPPING['id']], s[ServicesFeed.OCTOPUS_MAPPING['name']]) for s in services])

        # fetch executions
        executions = octopus.get_executions()
        print '  fetched', len(executions), 'service executions'
        sys.stdout.flush()

        # convert executions
        executions = [ExecutionsFeed.Execution(e, self.service_names) for e in executions]

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
