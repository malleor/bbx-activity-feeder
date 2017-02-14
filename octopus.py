import sys
from os import environ as env
import MySQLdb


class OctopusConnector(object):
    def __init__(self):
        self.host   = env['OCTOPUS_HOST']
        self.dbname = env['OCTOPUS_DB']
        self.user   = env['OCTOPUS_USER']
        passwd      = env['OCTOPUS_PASS']

        try:
            self.db = MySQLdb.connect(host=self.host, db=self.dbname, user=self.user, passwd=passwd, charset='utf8')
        except:
            self.db = None
            print 'OctopusConnector OFFLINE'
            sys.stdout.flush()
            return

        print 'OctopusConnector online'
        print '  database: %s @ %s' % (self.dbname, self.host)
        print '  using MySQLdb version', MySQLdb.__version__
        print '  ' + self.db.stat()
        sys.stdout.flush()

    def get_services(self):
        c = self.db.cursor()

        try:
            c.execute('''select * from service''')
            d = c.fetchall()
        except MySQLdb.OperationalError as err:
            print 'Failed to fetch services!'
            return []

        cols = [x[0] for x in c.description]
        d = [dict(zip(cols, x)) for x in d]

        return d

    def get_executions(self):
        c = self.db.cursor()

        try:
            c.execute('''select * from orders''')
            d = c.fetchall()
        except MySQLdb.OperationalError as err:
            print 'Failed to fetch executions!'
            return []

        cols = [x[0] for x in c.description]
        d = [dict(zip(cols, x)) for x in d]

        return d

    def get_clusters(self):
        c = self.db.cursor()

        try:
            c.execute('''select * from cluster''')
            d = c.fetchall()
        except MySQLdb.OperationalError as err:
            print 'Failed to fetch clusters!'
            return []

        cols = [x[0] for x in c.description]
        d = [dict(zip(cols, x)) for x in d]

        return d
