import sys


class OctopusConnector(object):
    def __init__(self):
        from os import environ as env
        import MySQLdb

        self.host   = env['OCTOPUS_HOST']
        self.dbname = env['OCTOPUS_DB']
        self.user   = env['OCTOPUS_USER']
        passwd      = env['OCTOPUS_PASS']

        try:
            self.db = MySQLdb.connect(host=self.host, db=self.dbname, user=self.user, passwd=passwd)
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

        c.execute('''select * from service''')
        d = c.fetchall()

        cols = [x[0] for x in c.description]
        d = [dict(zip(cols, x)) for x in d]

        return d

    def get_executions(self):
        c = self.db.cursor()

        c.execute('''select * from orders''')
        d = c.fetchall()

        cols = [x[0] for x in c.description]
        d = [dict(zip(cols, x)) for x in d]

        return d

    def get_clusters(self):
        c = self.db.cursor()

        c.execute('''select * from cluster''')
        d = c.fetchall()

        cols = [x[0] for x in c.description]
        d = [dict(zip(cols, x)) for x in d]

        return d
