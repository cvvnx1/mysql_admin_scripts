#!/usr/bin/env python

__title__ = "mysqlbackup"
__version__ = "0.1"
__author__ = "cvvnx1@163.com"
__email__ = "cvvnx1@163.com"
__source__ = "https://github.com/fr3nd/mysqlpdump/blob/master/mysqlpdump.py"

import sys
import os
import gzip
import tarfile
import commands
import time
import mysql.connector
import threading, Queue
from optparse import OptionParser

class Log:
    """Logging class"""
    def __init__(self, verbose):
        self.verbose = verbose

    def write(self, line):
        """Logs an especified line"""
        if self.verbose:
            sys.stderr.write (" - " + str(line) + "\n")

class Database:
    """Class to handle database connection and status"""
    def __init__(self, log, host, user, passwd, port='3306'):
        self.log = log
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = port
        self.log.write("Connection to database on %s." % self.host)
        try:
            self.db = mysql.connector.connect(host=self.host, user=self.user, passwd=self.passwd, port=self.port)
            self.cursor = self.db.cursor()
        except mysql.connector.Error as e:
            self.log.write("Mysql Error %d: %s." % (e.args[0], e.args[1]))
        self.log.write("Database connected")

    def close(self):
        self.log.write("Closing database connection.")
        self.db.close()

    def lockAll(self):
        """Locks all tables for read/write"""
        self.log.write("Locking all tables.")
        self.cursor.execute("FLUSH TABLES WITH READ LOCK;")

    def unlockAll(self):
        "Unlock all tables."
        self.log.write("Unlocking all tables.")
        self.cursor.execute("UNLOCK TABLES;")

    def unlockTable(self, table):
        self.log.write("Unlocking table %s" % table)
        self.cursor.execute("UNLOCK TABLES %s READ;" % table)

    def getDatabases(self, included, excluded):
        """Return all the databases. Included and excluded databases can be specified."""
        self.cursor.execute("SHOW DATABASES;")
        result = self.cursor.fetchall()
        databases = []
        for item in result:
            if len(included) == 0:
                if item[0] not in excluded:
                    databases.append(item[0])
            else:
                if (item[0] in included) and (item[0] not in excluded):
                    databases.append(item[0])
        return databases

    def getTables(self, database):
        """Return all tables for a given database"""
        self.cursor.execute("SHOW TABLES FROM " + str(database) + ";")
        result = self.cursor.fetchall()
        tables = []
        for item in result:
            tables.append(item[0])
        return tables

    def slaveStatus(self):
        """Return slave status"""
        self.cursor.execute("SHOW SLAVE STATUS;")
        result = self.cursor.fetchall()
        return result

    def setMaster(self, slave_status):
        try:
            return "CHANGE MASTER TO MASTER_HOST=\'" + slave_status[0][1] + "\', MASTER_LOG_FILE=\'" + slave_status[0][5] + "\', MASTER_LOG_POS=" + str(slave_status[0][6]) + ";"
        except:
            return ""

    def dump(self, database, table, tableList, destination, custom_parameters="", stdout=False, gzip=False, mysqldump="/usr/bin/mysqldump"):
        """Dump a specified table.
        It can dump it to a file or just return all the dumped data.
        It can waste a lot of memory if its returning a big table."""
        default_parameters = "--skip-lock-tables"
        cmd = mysqldump + " " + default_parameters
        if custom_parameters != "":
            cmd = cmd + " " + custom_parameters
        cmd = cmd + " -h" + self.host + " -P" + self.port + " -u" + self.user + " -p" + self.passwd + " " + database + " " + table
        if stdout:
            return commands.getstatusoutput(cmd)
        else:
            file = destination + "/" + database + "-" + table + ".sql"
            tableList.append(file)
            if gzip:
                cmd = cmd + " | gzip -c > " + file + ".gz"
            else:
                cmd = cmd + " > " + file
            os.system(cmd)
            return (None, None)

class Worker(threading.Thread):
    def __init__(self, queue, log, db, table_list, event_dict, destination, custom_parameters="", stdout=False, gzip=False, ):
        threading.Thread.__init__(self)
        self.queue = queue
        self.log =log
        self.db = db
        self.event_dict = event_dict
        self.stdout = stdout
        self.gzip = gzip
        self.destination = destination
        self.custom_parameters = custom_parameters
        self.table_list = table_list

    def run(self):
        self.log.write("Worker " + self.getName() + " started")
        while True:
            try:
                num, database, table = self.queue.get(True, 1)
            except Queue.Empty:
                break
            self.event_dict[num] = threading.Event()
            self.event_dict[num].clear()
            self.log.write(self.getName() + " dumping " + database + " " + table)
            status, output = self.db.dump(database, table, self.table_list, self.destination, self.custom_parameters, self.stdout, self.gzip, )
            if self.stdout:
                if num > 0:
                    while not self.event_dict[num - 1].isSet():
                        self.event_dict[num - 1].wait()
            self.log.write(self.getName() + " dumped " + database + " " + table)
            if output:
                print output
            self.event_dict[num].set()

def full_backup():
    


def main():
    try:
        current_user = os.getlogin()
    except:
        current_user = "nobody"

    # Get the runtime options
    usage = "usage: %prog [options]\n Run mysqldump in paralel"
    parser = OptionParser(usage, version=__version__)
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="Verbose output")
    parser.add_option("-t", "--type", action="store", dest="type", default='full', help="Type of backup: full or increment")
    parser.add_option("-u", "--user", action="store", dest="user", type="string", default=current_user, help="User for login.")
    parser.add_option("-p", "--password", action="store", dest="passwd", type="string", default='', help="Password for login.")
    parser.add_option("-H", "--host", action="store", dest="host", type="string", default='localhost', help="Connect to host.")
    parser.add_option("-P", "--port", action="store", dest="port", type="string", default='3306', help="Port for host.")
    parser.add_option("-T", "--threads", action="store", dest="threads", type="int", default=5, help="Threads used. Default = 5")
    parser.add_option("-s", "--stdout", action="store_true", dest="stdout", default=False, help="Output dumps to stdout instead to files. WARNING: It can exaust all your memory!")
    parser.add_option("-g", "--gzip", action="store_true", dest="gzip", default=False, help="Add gzip compression to files.")
    parser.add_option("-m", "--master-data", action="store_true", dest="master_data", default=False, help="This causes the binary log position and filename to be written to the file 00_master_position.sql.")
    parser.add_option("-d", "--destination", action="store", dest="destination", type="string", default=".", help="Path where to store generated dumps.")
    parser.add_option("-o", "--opts", action="store", dest="parameters", type="string", default="", help="Pass option parameters directly to mysqldump.")
    parser.add_option("-i", "--include_database", action="append", dest="included_databases", default=[], help="Databases to be dumped. By default, all databases are dumped. Can be called more than one time.")
    parser.add_option("-e", "--exclude_database", action="append", dest="excluded_databases", default=[], help="Databases to be excluded from the dump. No database is excluded by default. Can be called more than one time.")
    (options, args) = parser.parse_args()

    # Initial log
    log = Log(options.verbose)

    # Connect to database
    try:
        db = Database(log, options.host, options.user, options.passwd, options.port)
    except:
        parser.error("Cannot connect to database %s" % options.host)
    db.lockAll()

    # Save the master status
    if options.master_data:
        f = open(options.destination + '/00_master_position.sql', 'w')
        f.write(db.setMaster(db.slaveStatus()))
        f.write('\n')
        f.close()

    # [Full] Initial public queue for threads calls
    q = Queue.Queue()
    x = 0
    for database in db.getDatabases(options.included_databases, options.excluded_databases):
        for table in db.getTables(database):
            q.put([x, database, table])
            x = x + 1

    # [Full] Initial threads for mysqldump (this step can ouput sql files)
    event_dict = {}
    threads = []
    table_list = []
    x = 0
    for i in range(options.threads):
        threads.append(Worker(q, log, db, table_list, event_dict, options.destination, options.parameters, options.stdout, options.gzip))
        threads[x].setDaemon(True)
        threads[x].start()
        x = x + 1

    # [Full] Wait for all threads to finish
    for thread in threads:
        thread.join()

    # Unlock table and close database connection
    db.unlockAll()
    db.close()

    # Pack sql files
    t = time.localtime(time.time())
    cu_time = "%04d%02d%02d%02d%02d%02d" % (t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)
    cu_file = options.destination + "/" + options.host + "-full-" + cu_time +".tar.gz"
    log.write("Tar pack to " + cu_file + ".")
    tar = tarfile.open(cu_file, "w:gz")
    for name in table_list:
        log.write("Tar add " + name + ".")
        tar.add(name, os.path.basename(name))
        os.remove(name)

    tar.close()
    log.write("Tar pack complete.")

if __name__ == "__main__":
    main()
